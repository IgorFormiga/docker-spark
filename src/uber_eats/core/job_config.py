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

class SparkSessionFactory:
    """
    Factory for creating SparkSession instances

    This factory centralizes the creation of SparkSession objects, ensuring
    consistent configuration across the application.
    """
    def __init__(self, **config,) -> None:
        self._path = config["path"]
        self._config = self.register_config()
        self._appname = self._config.custom["app_name"]
        self._config_spark = self._config.spark
        self._spark = self._builder_spark(self._config_spark, self._appname)
        self._sources = self._register_sources(self._config.sources)
        self._sinks = self._register_sinks(self._config.sinks)
        self._custom = self._register_custom(self._config.custom)
    
    def register_config(self) -> Bunch:
        with open(f"{self._path}/config.yaml", "r") as file:
            config = yaml.safe_load(file)
        config_bunch = Bunch()
        for key, value in config.items():
            config_bunch[key] = Bunch()
            if isinstance(value, list):
                for item in value:
                    config_bunch[key][item["name"]] = item
            else:
                config_bunch[key] = value
        return config_bunch

    def _builder_spark(
        self,
        spark_conf: List[Dict[str, Any]] = None,
        app_name: Optional[str] = None
    ):
        spark_session_builder = SparkSession.builder.appName(app_name)
        for key, value in {**spark_conf["conf"]}.items():
            spark_session_builder.config(key,value)

        spark_session = spark_session_builder.getOrCreate()

        logger.info(f"spark config: {spark_session.sparkContext.getConf().getAll()}")
        spark_session.sparkContext.setLogLevel("INFO")

        return spark_session
    
    @property
    def spark(self) -> SparkSession:
        return self._spark
    
    @property
    def sources(self) -> Bunch:
        return self._sources
    
    @property
    def sinks(self) -> Bunch:
        return self._sinks
    
    @property
    def custom(self) -> Bunch:
        return self._custom

    def _register_sources(
        self,
        sources: Dict[str, Any]
    ):
        for table in sources:
            sources[table] = self.spark.read.format(sources[table]["format"]).load(sources[table]["path"])
        
        sources[table].show()
        sources[table].printSchema()
    # df = self.spark.read \
    # .format("csv") \
    # .options(**opcoes) \
    # .load("caminho/arquivo.csv")


    # formatos_validos = {
    #     "csv": {"sep", "header", "inferSchema", "encoding"},
    #     "parquet": {"mergeSchema"},
    #     "json": {"multiLine", "encoding"},
    # }

    # def carregar_com_filtro(spark, caminho, formato, opcoes):
    #     opcoes_validas = formatos_validos.get(formato, set())
    #     opcoes_filtradas = {k: v for k, v in opcoes.items() if k in opcoes_validas}
        
    #     return spark.read.format(formato).options(**opcoes_filtradas).load(caminho)

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