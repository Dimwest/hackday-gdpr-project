import pyspark
from configparser import ConfigParser
from pathlib import Path
from src.spark import df_join, get_source_ids, anonymize_df, show_examples
from  pyspark.sql.functions import input_file_name

if __name__ == '__main__':

    # Parse config
    cfg = ConfigParser()
    cfg.read(f'{Path(__file__).parent}/config.ini')

    # Create Spark configuration and SQLContext
    spark_cfg = pyspark.SparkConf().setAll([('spark.driver.host', cfg['sparkconfig']['host'])])
    sc = pyspark.SparkContext(conf=spark_cfg)
    sqlContext = pyspark.SQLContext(sc)

    # Create Spark Dataframe listing all user IDs to anonymize data from
    user_ids = get_source_ids(sqlContext, cfg)

    # Read target events JSON data into Dataframe
    events = sqlContext.read.json(cfg['anonymize']['target_dir'],
                                  multiLine=cfg.getboolean('anonymize', 'multiline')
                                  ).withColumn("filename", input_file_name())

    # Left join both Dataframes and flag events to anonymize
    events = df_join(events, user_ids, 'left', cfg)

    # Anonymize events data
    events = anonymize_df(events, cfg)

    show_examples(events, cfg)

