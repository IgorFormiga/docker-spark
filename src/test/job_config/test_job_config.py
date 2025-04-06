from src.utils.job_config import JobConfig
from src.utils.bunch import Bunch

class TestJobConfig:
    
    def test_register_config(self):
        config_bunch = JobConfig(path="src/test/job_config/").register_config()

        config_bunch_expected = Bunch(
            {
                'spark': Bunch({'conf': {'spark.sql.sources.partitionOverwriteMode': 'dynamic'}}), 
                'sources': Bunch({'tabelaA': Bunch({'name': 'tabelaA', 'format': 'x', 'path': 'y'})}), 
                'sinks': Bunch({'tabelaB': Bunch({'name': 'tabelaB', 'format': 'Y', 'mode': 'overwrite', 'partition_by': ['DATA_BASE'], 'path': 'Z'})}), 
                'custom': Bunch(
                    {
                        'ref_date': '2025-04-05', 
                        'app_name': 'src.trip_record_data.api_to_landing'
                    }
                )
            }
        )

        assert config_bunch == config_bunch_expected
