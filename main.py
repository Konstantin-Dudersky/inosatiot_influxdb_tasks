import getopt
import sys
import time
from datetime import datetime, timedelta

import enlighten
import pandas as pd
import schedule
import yaml
from influxdb_client import InfluxDBClient, WriteOptions
from loguru import logger

from config import Config

logger.remove()
logger.add(sys.stderr, level='INFO')
logger.add('logs/log.txt', level='INFO', rotation='5 MB')


def check_bucket(client: InfluxDBClient, bucket_name: str):
    try:
        bucket = client.buckets_api().find_bucket_by_name(bucket_name)
    except Exception as e:
        logger.critical(e)
        sys.exit(1)

    if bucket is None:
        logger.warning(f"bucket {bucket_name} in host {client.url} not found")
        bucket = client.buckets_api().create_bucket(bucket_name=bucket_name)
        logger.success(f"bucket {bucket.name} in host {client.url} created")


def query_count(client: InfluxDBClient, bucket: str, measurement: str, start: datetime, stop: datetime):
    if measurement == '':
        query = f"""
            from(bucket: "{bucket}")
              |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
              |> count()
              |> group(columns: ["_start", "_stop",])
              |> sum()
              |> yield()
        """
    else:
        query = f"""
            from(bucket: "{bucket}")
              |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
              |> filter(fn: (r) => r["_measurement"] == "{measurement}")
              |> count()
              |> group(columns: ["_start", "_stop",])
              |> sum()
              |> yield()
        """
    df = client.query_api().query_data_frame(query)

    if len(df) == 0:
        return 0
    else:
        return df.loc[0, '_value']


def pdperdiod_to_fluxperiod(period: str):
    s = period.replace('T', 'm')
    return s


def mirror(period: timedelta, bsize: int = 10000):
    stop = datetime.now().astimezone()
    start = stop - period

    # read settings
    with open('../config_inosatiot_influxdb_mirror.yaml') as stream:
        config = yaml.safe_load(stream)
    logger.info('----------')
    logger.info(f'mirror values in range {start.isoformat()} - {stop.isoformat()}')

    for mir in config['mirroring']:
        src_host_url = mir['src_host']['url']
        src_host_org = mir['src_host']['org']
        src_host_token = mir['src_host']['token']
        src_bucket = mir['src_bucket']

        dst_host_url = mir['dst_host']['url']
        dst_host_org = mir['dst_host']['org']
        dst_host_token = mir['dst_host']['token']
        dst_bucket = mir['dst_bucket']

        logger.info('----------')

        logger.info(f"source host: {src_host_url}; source bucket: {src_bucket}")
        client_src = InfluxDBClient(
            url=src_host_url,
            token=src_host_token,
            org=src_host_org
        )

        logger.info(f"destination host: {dst_host_url}; destination bucket: {dst_bucket}")
        client_dst = InfluxDBClient(
            url=dst_host_url,
            token=dst_host_token,
            org=dst_host_org
        )

        # сравниваем общее кол-во точек данных
        try:
            count_src_all = query_count(
                client=client_src,
                bucket=src_bucket,
                measurement='',
                start=start,
                stop=stop,
            )
        except Exception as e:
            logger.critical(e)
            sys.exit(1)

        try:
            count_dst_all = query_count(
                client=client_dst,
                bucket=dst_bucket,
                measurement='',
                start=start,
                stop=stop,
            )
        except Exception as e:
            logger.critical(e)
            sys.exit(1)

        logger.info(f'count in source bucket: {count_src_all}, count in destination bucket: {count_dst_all}')
        if count_dst_all >= count_src_all:
            logger.info(f'no need mirroring bucket: {src_bucket}')
            continue

        progress_bar = enlighten.Counter(
            total=count_src_all,
            desc=f"{dst_host_url} | {dst_bucket} |",
            unit='points')

        # запрашиваем _measurement
        query = f"""
            import "influxdata/influxdb/schema"
            schema.measurements(bucket: "{src_bucket}")
        """

        for meas in client_src.query_api().query_data_frame(query)['_value']:
            logger.debug(f'_measurement: {meas}')

            # Запрашиваем кол-во точек данных
            count_src_meas = query_count(
                client=client_src,
                bucket=src_bucket,
                measurement=meas,
                start=start,
                stop=stop,
            )

            periods = int(count_src_meas / bsize)
            periods = 2 if periods < 2 else periods
            logger.debug(f"divide on periods: {periods}")

            dt = pd.date_range(start=start, end=stop, periods=periods)

            for i in range(len(dt) - 1):
                start_batch = dt[i]
                stop_batch = dt[i + 1]
                logger.debug(f'start: {start_batch.isoformat()}, stop: {stop_batch.isoformat()}')

                count_src = query_count(
                    client=client_src,
                    bucket=src_bucket,
                    measurement=meas,
                    start=start_batch,
                    stop=stop_batch,
                )

                count_dst = query_count(
                    client=client_dst,
                    bucket=dst_bucket,
                    measurement=meas,
                    start=start_batch,
                    stop=stop_batch,
                )

                logger.debug(f'count in local bucket: {count_src}; count in remote bucket: {count_dst}')
                progress_bar.update(count_src)

                if count_dst >= count_src:
                    continue

                query = f"""
                from(bucket: "{src_bucket}")
                  |> range(start: {start_batch.isoformat()}, stop: {stop_batch.isoformat()})
                  |> filter(fn: (r) => r["_measurement"] == "{meas}")
                  |> yield()"""

                df_list = client_src.query_api().query_data_frame(query)

                if type(df_list) is list:
                    pass
                else:
                    if len(df_list) == 0:
                        continue
                    df_list = [df_list]

                logger.debug(f'loaded dataframe: {len(df_list)}')

                with client_dst.write_api(
                        write_options=WriteOptions(
                            batch_size=1000,
                            flush_interval=1000,
                            jitter_interval=0,
                            retry_interval=5_000,
                            max_retries=5,
                            max_retry_delay=30_000,
                            exponential_base=2)) as _write_client:
                    for df in df_list:
                        _fields = df['_field'].unique()

                        tags = []
                        for col in df.columns:
                            if col not in _fields:
                                tags.append(col)

                        datatype = df.loc[0, 'datatype']
                        df['_value'] = df['_value'].astype(datatype)

                        for _field in _fields:
                            df_temp = df[df['_field'] == _field]
                            df_temp = df_temp.rename(columns={'_value': _field})
                            df_temp = df_temp.drop(
                                columns=['_start', '_stop', 'result', 'table', '_measurement', '_field', ])
                            df_temp = df_temp.set_index("_time")

                            _write_client.write(bucket=dst_bucket,
                                                org=dst_host_org,
                                                record=df_temp,
                                                data_frame_measurement_name=meas,
                                                data_frame_tag_columns=tags)

            logger.success(f"mirror measurement {meas} completed")

        logger.success(f"mirror bucket {src_bucket} to host {dst_host_url} completed")


def downsampling(start: datetime, stop: datetime, src_client: InfluxDBClient, src_bucket: str,
                 dst_client: InfluxDBClient, dst_bucket: str, window: str):
    aggfuncs = ['first', 'increase', 'max', 'mean', 'min', 'sum']
    every = pdperdiod_to_fluxperiod(window)

    for aggfunc in aggfuncs:

        if aggfunc == 'first':
            continue
        elif aggfunc == 'increase':
            query = f"""
                import "strings"

                from(bucket: "{src_bucket}")
                    |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
                    |> filter(fn: (r) => strings.containsStr(v: r["aggfunc"], substr: "increase"))
                    |> increase()
                    |> last()
                    |> map(fn: (r) => ({{r with
                        aggwindow: "{window}",
                        aggfunc: "sum" 
                    }}))
                    |> drop(columns: ["_time"])
                    |> yield()
            """
            query = f"""
                import "strings"
            
                from(bucket: "{src_bucket}")
                    |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
                    |> filter(fn: (r) => strings.containsStr(v: r["aggfunc"], substr: "increase"))
                    |> aggregateWindow(
                        every: {every},
                        fn: (column, tables=<-) => tables
                            |> increase()
                            |> last(),
                        createEmpty: false)
                    |> map(fn: (r) => ({{r with
                        aggwindow: "{window}",
                        aggfunc: "sum"
                    }}))
                    |> yield()
            """
        elif aggfunc == 'max':
            query = f"""
                import "strings"

                from(bucket: "{src_bucket}")
                    |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
                    |> filter(fn: (r) => strings.containsStr(v: r["aggfunc"], substr: "max"))
                    |> aggregateWindow(every: {every}, fn: max, createEmpty: false)
                    |> map(fn: (r) => ({{r with 
                        aggwindow: "{window}",
                        aggfunc: "max"
                    }}))
                    |> group(columns: ["_start", "_stop"])
                    |> yield(name: "max")
            """
        elif aggfunc == 'mean':
            query = f"""
                import "strings"
            
                data = from(bucket: "{src_bucket}")
                    |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
                    |> filter(fn: (r) => strings.containsStr(v: r["aggfunc"], substr: "mean"))
                    |> aggregateWindow(every: {every}, fn: mean, createEmpty: false)
                    |> map(fn: (r) => ({{r with 
                        aggwindow: "{window}",
                        aggfunc: "mean"
                    }}))
                    |> group(columns: ["_start", "_stop"])
                    |> yield(name: "mean")
            """
        elif aggfunc == 'min':
            query = f"""
                import "strings"

                data = from(bucket: "{src_bucket}")
                    |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
                    |> filter(fn: (r) => strings.containsStr(v: r["aggfunc"], substr: "min"))
                    |> min()
                    |> map(fn: (r) => ({{r with 
                        aggwindow: "{window}",
                        aggfunc: "min"
                    }}))
                    |> group(columns: ["_start", "_stop"])
                    |> drop(columns: ["_time"])
                    |> yield(name: "min")
            """

            query = f"""
                import "strings"

                data = from(bucket: "{src_bucket}")
                    |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
                    |> filter(fn: (r) => strings.containsStr(v: r["aggfunc"], substr: "min"))
                    |> aggregateWindow(every: {every}, fn: min, createEmpty: false)
                    |> map(fn: (r) => ({{r with 
                        aggwindow: "{window}",
                        aggfunc: "min"
                    }}))
                    |> group(columns: ["_start", "_stop"])
                    |> yield(name: "min")
            """
        elif aggfunc == 'sum':
            continue
        else:
            continue
        df_list = src_client.query_api().query_data_frame(query)

        if type(df_list) is list:
            pass
        else:
            if len(df_list) == 0:
                return
            df_list = [df_list]

        logger.debug(f'loaded dataframe: {len(df_list)}')

        with dst_client.write_api(
                write_options=WriteOptions(
                    batch_size=1000,
                    flush_interval=1000,
                    jitter_interval=0,
                    retry_interval=5_000,
                    max_retries=5,
                    max_retry_delay=30_000,
                    exponential_base=2)) as _write_client:

            for df in df_list:
                _meass = df['_measurement'].unique()
                _fields = df['_field'].unique()

                tags = []
                for col in df.columns:
                    if col not in _fields:
                        tags.append(col)

                datatype = df.loc[0, 'datatype']
                df['_value'] = df['_value'].astype(datatype)

                for _meas in _meass:
                    df_meas = df[df['_measurement'] == _meas]

                    for _field in _fields:
                        df_field = df_meas[df_meas['_field'] == _field]
                        df_field = df_field.rename(columns={'_value': _field})
                        df_field = df_field.drop(
                            columns=['_start', '_stop', 'result', 'table', '_measurement', '_field', ])
                        df_field = df_field.set_index("_time")

                        _write_client.write(
                            bucket=dst_bucket,
                            record=df_field,
                            data_frame_measurement_name=_meas,
                            data_frame_tag_columns=tags)

        logger.trace(f'downsample for {aggfunc} complete')


def task_downsampling(period: timedelta, batch: str, aggwindow: str,
                      src_client: InfluxDBClient, src_bucket: str,
                      dst_client: InfluxDBClient, dst_bucket: str):
    stop = datetime.now().astimezone()
    start = stop - period

    idx = pd.period_range(
        start=pd.to_datetime(start).floor(freq=aggwindow),
        end=pd.to_datetime(stop).floor(freq=aggwindow),
        freq=batch)

    t = time.perf_counter()
    for i in range(len(idx) - 1):
        start = idx[i].to_timestamp().tz_localize('Europe/Minsk')
        stop = idx[i + 1].to_timestamp().tz_localize('Europe/Minsk')
        downsampling(start=start, stop=stop, src_client=src_client, src_bucket=src_bucket,
                     dst_client=dst_client, dst_bucket=dst_bucket, window=aggwindow)

    logger.info(
        f"Downsampling task completed; start: {start.isoformat()}, stop: {stop.isoformat()}, aggwindow: {aggwindow}")
    logger.info(f"Time elapsed: {time.perf_counter() - t}s")


if __name__ == '__main__':
    opts, args = getopt.getopt(sys.argv[1:], "hm:b:", ['help', 'mode=', 'start=', 'stop=', ])

    mode = None
    start = None
    stop = None

    for opt, arg in opts:

        if opt in ['-h', '--help']:
            print(f"""
Tasks for influxdb

arguments:
    -h, --help : show help
    --mode (required) : mode
        rt - cyclical execute in real-time
        man - manual process data (--start, --stop)
    --start <datetime in isoformat> (required for man mode):
        start timestamp
    --stop <datetime in isoformat> (optional for man mode):
        stop timestamp. If not specified, the current time is used.

examples:
    venv/bin/python3 main.py --mode rt
    venv/bin/python3 main.py --mode man --start 2021-01-01T00:00:00+03:00
    venv/bin/python3 main.py --mode man --start 2021-01-01T00:00:00+03:00 --stop 2022-01-01T00:00:00+03:00
""")
            sys.exit()
        elif opt == '--mode':
            if arg in ['rt', 'man']:
                mode = arg

        elif opt == '--start':
            try:
                start = datetime.fromisoformat(arg)
            except ValueError as ve:
                print(f'Invalid --start value:\n{ve}')
                sys.exit()

        elif opt == '--stop':
            try:
                stop = datetime.fromisoformat(arg)
            except ValueError as ve:
                print(f'Invalid --stop value:\n{ve}')
                sys.exit()

    if mode is None:
        print(f"Invalid --mode value, see --help")
        sys.exit()
    elif mode == 'rt':
        print('Start rt mode, press CTRL-C for exit')
    elif mode == 'man':
        if start is None:
            print(f"Value --start required for man mode, exit")
            sys.exit(1)
        if stop is None:
            stop = datetime.now().astimezone()

        if start.tzname() is None:
            local_tz = datetime.now().astimezone().tzinfo
            start = start.astimezone(local_tz)

        if stop.tzname() is None:
            local_tz = datetime.now().astimezone().tzinfo
            stop = stop.astimezone(local_tz)

        print(f"Start man mode, start:{start.isoformat()}, stop:{stop.isoformat()}")

    config = Config('../config_inosatiot_influxdb_tasks.yaml')

    for ds in config.downsampling:

        src_client = InfluxDBClient(
            url=ds.src_host.url,
            token=ds.src_host.token,
            org=ds.src_host.org,
        )

        dst_client = InfluxDBClient(
            url=ds.dst_host.url,
            token=ds.dst_host.token,
            org=ds.dst_host.org,
        )

        # delete bucket
        # dst_client.buckets_api().delete_bucket(dst_client.buckets_api().find_bucket_by_name(ds['dst_bucket']))

        check_bucket(dst_client, ds.dst_bucket)

        for aggwindow in ds.aggwindows:
            temp_period = pd.period_range(end=datetime.now(), periods=2, freq=aggwindow)
            total_seconds = (temp_period[1].to_timestamp() - temp_period[0].to_timestamp()).total_seconds()

            if mode == 'rt':
                if total_seconds <= 60 * 60:
                    schedule.every(1).hours.at(':00').do(
                        task_downsampling,
                        period=timedelta(hours=1), batch='1H', aggwindow=aggwindow,
                        src_client=src_client, src_bucket=ds.src_bucket,
                        dst_client=dst_client, dst_bucket=ds.dst_bucket
                    )

                    schedule.every(1).days.at('00:01').do(
                        task_downsampling,
                        period=timedelta(hours=24), batch='4H', aggwindow=aggwindow,
                        src_client=src_client, src_bucket=ds.src_bucket,
                        dst_client=dst_client, dst_bucket=ds.dst_bucket
                    )
                elif total_seconds <= 60 * 60 * 24:
                    pass
            elif mode == 'man':
                if total_seconds <= 60 * 60:
                    schedule.every(1).hours.at(':00').do(
                        task_downsampling,
                        period=stop - start, batch='8H', aggwindow=aggwindow,
                        src_client=src_client, src_bucket=ds.src_bucket,
                        dst_client=dst_client, dst_bucket=ds.dst_bucket
                    )

    if mode == 'rt':
        while True:
            # schedule.run_all(delay_seconds=10)
            schedule.run_pending()
            time.sleep(1)
    elif mode == 'man':
        logger.info(f"Start manual downsampling")

        schedule.run_all(delay_seconds=10)

# TODO - предусмотреть возможость запуска из командной строки для ручной синхронизации заданного промежутка времени


# schedule.every(5).minutes.do(mirror, period=timedelta(minutes=10), bsize=10000)
# schedule.every(1).hours.at(':00').do(mirror, period=timedelta(days=2), bsize=10000)
# schedule.every(1).days.at('00:05').do(mirror, period=timedelta(days=60), bsize=10000)
