import sys
import time
from datetime import datetime, timedelta

import enlighten
import pandas as pd
import schedule
import yaml
from influxdb_client import InfluxDBClient, WriteOptions
from loguru import logger

logger.remove()
logger.add(sys.stderr, level='INFO')
logger.add('logs/log.txt', level='INFO', rotation='5 MB')


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

                with client_dst.write_api(write_options=WriteOptions(
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


if __name__ == '__main__':
    schedule.every(5).minutes.do(mirror, period=timedelta(minutes=10), bsize=10000)
    schedule.every(1).hours.at(':00').do(mirror, period=timedelta(days=2), bsize=10000)
    schedule.every(1).days.at('00:05').do(mirror, period=timedelta(days=60), bsize=10000)

    while True:
        schedule.run_all(delay_seconds=10)
        schedule.run_pending()
        time.sleep(1)

# TODO - предусмотреть возможость запуска из командной строки для ручной синхронизации заданного промежутка времени
