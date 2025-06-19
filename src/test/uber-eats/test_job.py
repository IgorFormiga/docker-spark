#from src.utils.job_config import JobConfig
import os
from src.utils.bunch import Bunch
from src.uber_eats.main import main, parse_args
from src.uber_eats.config.config import load_config
from src.uber_eats.core.session import SparkSessionFactory
from pyspark.sql import SparkSession
from pyspark.sql.types import(
    IntegerType,
    StringType,
    StructField,
    StructType
)

class TestJob:
    config_dev = load_config("dev")
    config_prd = load_config("prod")

    def test_load_config(self):
        expected_config = {
            'dev': {
                'app': {'name': 'uber-eats-orders-pipeline', 'log_level': 'INFO'}, 
                'spark': {
                    'log_level': 'WARN', 
                    'spark.sql.shuffle.partitions': 10, 
                    'spark.sql.adaptive.enabled': True, 
                    'master': 'local[*]', 
                    'spark.driver.memory': '1g', 
                    'spark.executor.memory': '1g'
                }, 
                'paths': {
                    'input': '/opt/bitnami/spark/jobs/uber-eats/data/orders.json', 
                    'output': '/opt/bitnami/spark/jobs/uber-eats/output/dev/orders'
                }
            }, 
            'prod': {
                'app': {'name': 'uber-eats-orders-pipeline', 'log_level': 'INFO'}, 
                'spark': {
                    'log_level': 'WARN', 
                    'spark.sql.shuffle.partitions': 50, 
                    'spark.sql.adaptive.enabled': True, 
                    'master': 'spark://spark-master:7077', 
                    'spark.driver.memory': '1g', 
                    'spark.executor.memory': '2g', 
                    'spark.executor.instances': 2
                }, 
                'paths': {
                    'input': '/opt/bitnami/spark/jobs/uber-eats/data/orders.json', 
                    'output': '/opt/bitnami/spark/jobs/uber-eats/output/prod/orders'
                }
            }
        }
        assert self.config_dev == expected_config["dev"]
        assert self.config_prd == expected_config["prod"]

    def test_register_spark(self):
        spark = SparkSessionFactory.create_session(
            self.config_dev["app"]["name"],
            self.config_dev
        )

        configs_return = {
            "master": spark.conf.get("spark.master"),
            'shuffle_partitions': spark.conf.get("spark.sql.shuffle.partitions"),
            'adaptive_execution': spark.conf.get("spark.sql.adaptive.enabled"),
            'driver_memory': spark.conf.get("spark.driver.memory"),
            'executor_memory': spark.conf.get("spark.executor.memory"),
        }


        expected_config = {
            'master': 'local[*]',
            'shuffle_partitions': '10', 
            'adaptive_execution': 'true', 
            'driver_memory': '1g', 
            'executor_memory': '1g'
        }
        assert configs_return == expected_config

