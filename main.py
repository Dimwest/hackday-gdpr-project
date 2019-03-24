import pyspark
import logging
from configparser import ConfigParser
from pathlib import Path
from src.config import validate_config
from src.spark import df_join, get_source_ids, anonymize_df, show_examples, write_events
from src.s3 import create_anonymized_bucket
from src.paths import split_path
from pyspark.sql.functions import input_file_name, col

if __name__ == '__main__':

    # Setup logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.WARN)

    # Parse and validate config
    cfg = ConfigParser()
    cfg.read(f'{Path(__file__).parent}/config.ini')
    validate_config(cfg, local_mode=True)
    logger.warning("--> Configuration file successfully parsed and validated")

    # Set Spark config
    spark_cfg_vars = [('spark.driver.host', cfg['sparkconfig']['host'])]

    # Set Spark configuration
    spark_cfg = pyspark.SparkConf().setAll(spark_cfg_vars)
    sc = pyspark.SparkContext(conf=spark_cfg)

    # Create SQL context
    sqlContext = pyspark.SQLContext(sc)

    # Get source bucket name and generate target bucket name
    bucket = split_path(cfg['anonymize']['target_objects_s3_path'])[1]
    new_bucket_name = f"{bucket}__anonymized"

    # Create target buckets
    # create_anonymized_bucket(new_bucket_name)
    logger.warning(f"--> New bucket {new_bucket_name} receiving anonymized events "
                   f"was successfully created")

    # Create Spark Dataframe listing all user IDs to anonymize data from
    user_ids = get_source_ids(sqlContext, cfg)
    logger.warning(f"--> Source IDs successfully fetched from "
                   f"{cfg['anonymize']['source_ids_s3_path']}")

    # Read target events JSON data into Dataframe
    events = sqlContext.read.json(cfg['anonymize']['target_objects_s3_path'],
                                  multiLine=cfg.getboolean('anonymize', 'multiline')
                                  ).withColumn("filename", input_file_name())
    logger.warning(f"--> JSON data to anonymize successfully fetched from "
                   f"{cfg['anonymize']['target_objects_s3_path']}")

    # Left join both Dataframes and flag events to anonymize
    events = df_join(events, user_ids, 'left', cfg)

    # Anonymize events data
    logger.warning(f"--> Starting data anonymization ...")
    events = anonymize_df(events, cfg)
    logger.warning(f"--> Data anonymization completed successfully")

    show_examples(events, cfg)

    logger.warning(f"--> Writing anonymized events "
                   f"to new bucket {new_bucket_name} ...")
    # write_events(events, cfg)
    logger.warning(f"--> Anonymized events successfully written "
                   f"to new bucket {new_bucket_name}")
