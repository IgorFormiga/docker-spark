import logging
import yaml
from src.utils.bunch import Bunch
from typing import Any, Dict, List, Optional, Union
from pyspark.sql import SparkSession


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

logger = logging.getLogger('spark_app')
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.propagate = True

class JobConfig():
    def __init__(self, **config,) -> None:
        self._path = config["path"]
        self._config = self.register_config()
        # self.spark = self._register_spark(self, self._config["spark"]["conf"], self._config["app_name"])
        # self.sources = self._register_sources(self,  self._config["sources"])
        # self.sinks = self._register_sinks(self, self._config["sinks"])
        # self.custom = self._register_custom(self, self._config["custom"])
    
    def register_config(self) -> Bunch:
        with open(f"{self._path}/config.yaml", "r") as file:
            config = yaml.safe_load(file)
        config_bunch = Bunch()
        for key, value in config.items():
            config_bunch[key] = Bunch()
            if isinstance(value, list):
                for item in value:
                    config_bunch[key][item["name"]] =  item
            else:
                config_bunch[key] = value
        return config_bunch

    def _register_spark(
        self,
        spark_conf: List[Dict[str, Any]] = None,
        app_name: Optional[str] = None
    ):
        pass
        # spark_session_builder = SparkSession.builder.appName(app_name)
        # for key, value in {**spark_conf}.items():
        #     spark_session_builder.config(key,value)

        # spark_session = spark_session_builder.getOrCreate()

        # logger.info(f"spark config: {spark_session.sparkContext.getConf().getAll()}")
        # spark_session.sparkContext.setLogLevel("INFO")

        # return spark_session
    
    
    def _register_sources(
        self,
        sources: List[Dict[str, Any]]
    ):
        pass

    def _register_sinks(
        self,
        sinks: List[Dict[str, Any]]
    ):
        pass

    def _register_custom(
        self,
        custom: List[Dict[str, Any]]
    ):
        pass