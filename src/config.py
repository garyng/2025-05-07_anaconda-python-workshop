import pathlib

from pydantic import BaseModel


class AppConfig(BaseModel):
    FundNameMappings: dict[str, str]
    FundReportsDirPath: str
    RefDbConnectionString: str
    ReconReportOutputPath: str


def load_app_config() -> AppConfig:
    config_path = pathlib.Path.cwd() / "configs" / "config.json"
    return AppConfig.model_validate_json(config_path.read_text())
