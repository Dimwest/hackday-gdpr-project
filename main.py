import pyspark
from pyspark.sql.functions import col, lit
from configparser import ConfigParser
from cryptography.fernet import Fernet
from pathlib import Path


def get_source_ids(sql_ctx, cfg):

    source_id = cfg['anonymize']['source_id_key']
    df = sql_ctx.read.json(
        cfg['anonymize']['source_dir'],
        multiLine=cfg.getboolean('anonymize', 'multiline')).withColumn('in_source_ids', lit(1)).sample(
        False, 0.3, seed=0)
    return df.where(col(source_id).isNotNull()).select(source_id, 'in_source_ids').distinct()


if __name__ == '__main__':

    # Read config
    cfg = ConfigParser()
    cfg.read(f'{Path(__file__).parent}/config.ini')

    # Creating Spark SQLContext
    sc = pyspark.SparkContext()
    sqlContext = pyspark.SQLContext(sc)

    source_ids = get_source_ids(sqlContext, cfg)
    source_ids.show(10)

    # Reading json data into a dataframe
    target_events = sqlContext.read.json(cfg['anonymize']['target_dir'], multiLine=cfg.getboolean('anonymize', 'multiline'))
    join_cond = [source_ids[cfg['anonymize']['source_id_key']]
                 == target_events[cfg['anonymize']['target_id_key']]]
    target_events.join(source_ids, join_cond, 'left').select('email', 'in_source_ids').show(1000)

    # Create encryption key
    key = Fernet.generate_key()

    target_events.printSchema()
