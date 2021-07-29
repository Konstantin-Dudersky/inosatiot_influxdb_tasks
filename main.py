import sys

from influxdb_client import InfluxDBClient, WriteOptions
from loguru import logger

# future settings file
influxdb_src_url = "http://localhost:8086"
influxdb_src_org = "Inosat"
influxdb_src_token = "KKIIV60BcG5u8BYRcPMc3EaZLcvKj73FWg0i3DXkmWUQ5enQtotEVK0VNbiNgTCGQL1bz5z-mOLXoaV_Puv5XQ=="
influxdb_src_bucket = "inosatiot_resources_sim"

influxdb_dst_url = "http://localhost:8086"
influxdb_dst_org = "Inosat"
influxdb_dst_token = "KKIIV60BcG5u8BYRcPMc3EaZLcvKj73FWg0i3DXkmWUQ5enQtotEVK0VNbiNgTCGQL1bz5z-mOLXoaV_Puv5XQ=="
influxdb_dst_bucket = "inosatiot_resources_sim_mirror"

query = f"""
from(bucket: "inosatiot_resources_sim")
  |> range(start: -1d)
  |> filter(fn: (r) => r["_measurement"] == "counter1")

  |> yield(name: "mean")"""

#   |> filter(fn: (r) => r["_field"] == "p")

client = InfluxDBClient(
    url=influxdb_src_url,
    token=influxdb_src_token,
    org=influxdb_src_org
)

df_list = client.query_api().query_data_frame(query)
print(len(df_list))
if type(df_list) is list:
    pass
else:
    df_list = [df_list]


for df in df_list:
    fields = df['_field'].unique()

    for field in fields:
        df[field] = df[df['_field'] == field]['_value']

    # field = df.loc[0, '_field']
    # df = df.rename(columns={'_value': field})
    df = df.drop(columns=['_start', '_stop', "result", "table", "_measurement", '_field', '_value'])
    df = df.set_index("_time")
    tags = []
    for col in df.columns:
        if col not in fields:
            tags.append(col)

    # df.info()
    print(df.head())

    with client.write_api(write_options=WriteOptions(batch_size=500,
                                                     flush_interval=10_000,
                                                     jitter_interval=2_000,
                                                     retry_interval=5_000,
                                                     max_retries=5,
                                                     max_retry_delay=30_000,
                                                     exponential_base=2)) as _write_client:
        _write_client.write(bucket="inosatiot_resources_sim_mirror",
                            org=influxdb_dst_org,
                            record=df,
                            data_frame_measurement_name='counter1',
                            data_frame_tag_columns=tags)

# tables = client.query_api().query(query)
# for table in tables:
#     print(table)
#     for row in table.records:
#         print(row.values)

sys.exit()


