import yaml


class Config:
    def __init__(self, file: str):
        with open(file) as stream:
            self._config = yaml.safe_load(stream)

    def __str__(self):
        return yaml.dump(self._config, default_flow_style=False)

    @property
    def downsampling(self):
        return self._config['downsampling']
