from __future__ import annotations
import os
import yaml
import warnings

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
    telegram_token: str

    @classmethod
    def from_yaml(cls, path: str):
        with open(path, "r") as f:
            return cls.parse_obj(yaml.safe_load(f))


def get_config(path: str | None = None) -> Config:
    if path is None:
        path = os.getenv("CONFIG_PATH", "config.yaml")
        if path == "config.yaml":
            warnings.warn("Config path not found in environment variables, using default 'config.yaml'")
    return Config.from_yaml(path)
