import yaml

from pydantic import BaseModel

class MariaDBConfig(BaseModel):
    user: str
    password: str
    host: str
    port: int

class MegaConfig(BaseModel):
    email: str
    password: str

class Config(BaseModel):
    artifacts_dir: str
    mega: MegaConfig
    mariadb: MariaDBConfig

    @classmethod
    def from_yaml(cls, path: str):
        with open(path, "r") as f:
            return cls.model_validate(yaml.safe_load(f), strict=True)


def get_config(path: str = "config.yaml") -> Config:
    return Config.from_yaml(path)
