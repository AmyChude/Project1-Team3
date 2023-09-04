import os
from pathlib import Path
import yaml
from typing import Union

def get_env_variable(var_name: str) -> str:
    """Get the environment variable or raise exception if not found"""
    value = os.environ.get(var_name)
    if value is None:
        raise Exception(f"Environment variable {var_name} not found")
    return value


def load_pipeline_config(pipeline_config_path: Union[Path, str]) -> dict:
    """Load the pipeline config from a yaml file"""
    if isinstance(pipeline_config_path, str):
        pipeline_config_path = Path(pipeline_config_path)
    if not pipeline_config_path.exists():
        raise Exception(f"Pipeline file {pipeline_config_path} not found")
    if pipeline_config_path.suffix != ".yaml":
        raise Exception(f"Pipeline file {pipeline_config_path} must be a .yaml file")
    with open(pipeline_config_path) as yaml_file:
        pipeline_config = yaml.safe_load(yaml_file)
        if not isinstance(pipeline_config, dict):
            raise Exception(f"Pipeline file {pipeline_config_path} is empty or invalid YAML file")
        return pipeline_config