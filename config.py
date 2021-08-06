import yaml


class Host:
    def __init__(self, url: str, token: str, org: str):
        self.token = token
        self.org = org
        self.url = url


class DownsamplingItem:
    def __init__(self, src_host: Host, src_bucket: str, dst_host: Host, dst_bucket: str, aggwindows: list):
        self.aggwindows = aggwindows
        self.dst_bucket = dst_bucket
        self.dst_host = dst_host
        self.src_bucket = src_bucket
        self.src_host = src_host


class Config:
    def __init__(self, file: str):
        with open(file) as stream:
            self._config = yaml.safe_load(stream)

        self._downsampling = []
        for ds in self._config['downsampling']:
            self._downsampling.append(
                DownsamplingItem(
                    Host(ds['src_host']['url'], ds['src_host']['token'], ds['src_host']['org']), ds['src_bucket'],
                    Host(ds['dst_host']['url'], ds['dst_host']['token'], ds['dst_host']['org']), ds['src_bucket'],
                    ds['aggwindows'])
            )

    def __str__(self):
        return yaml.dump(self._config, default_flow_style=False)

    @property
    def downsampling(self):
        return self._downsampling
