import pyspark
import ujson
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, lit, when, udf, to_json, from_json
from configparser import ConfigParser
from pathlib import Path
from utils.spark import df_join
from utils.anonymize import anonymize_nested_value, anonymize_value


def get_source_ids(sql_ctx, cfg):

    """
    Fetches user IDs from specified JSON file

    :param sql_ctx: PySpark SQL context
    :param cfg: ConfigParser object

    :return: Dataframe made of two columns, the "id" column,
    and "in_source_ids" column, always equal to 1.
    """

    source_id = cfg['anonymize']['source_id_key']
    df = sql_ctx.read.json(
        cfg['anonymize']['source_dir'],
        multiLine=cfg.getboolean('anonymize', 'multiline')).withColumn('in_source_ids', lit(1)).sample(
        False, 0.3, seed=0)
    return df.where(col(source_id).isNotNull()).select(source_id, 'in_source_ids').distinct()


def anonymize_json(json_str, target_col, encoding):

    """
    Convert JSON string to Python dict, anonymize target column,
    then convert back to JSON string

    :param json_str: JSON string to operate on
    :param target_col: column to anonymize
    :param encoding: configured encoding for handling text data

    :return: updated JSON string
    """

    target_col = target_col.split('.')[-1]
    nested_json = ujson.loads(json_str.encode(encoding))
    anonymize_nested_value(nested_json, target_col.split('.'), encoding)
    return ujson.dumps(nested_json)


def anonymize_nested_field(df, col_name, encoding):

    """
    Update Dataframe nested field in several steps:
        1) Converts the column values to JSON strings
        2) Creates a new temp column named col_name__temp from a
         UDF anonymizing JSON data
        3) Converts JSON string back to StructType/ArrayType
        4) Drop old column and rename temp column

    :param df: Dataframe object to operate on
    :param col_name: column to anonymize
    :param encoding: configured encoding for handling text data

    :return: updated Dataframe object
    """

    root_col = col_name.split('.')[0]
    new_name = root_col + "___temp"
    schema = df.select(root_col).schema.fields[0].dataType
    udf_anonymize = udf(lambda x: anonymize_json(x, col_name, encoding) if x else x, StringType())

    df = df.withColumn(new_name, when(df['in_source_ids'].isNotNull(),
                                      from_json(udf_anonymize(to_json(col(root_col))), schema)))

    df = df.drop(root_col)
    df = df.withColumnRenamed(new_name, root_col)
    return df


def anonymize_top_level_field(df, column, encoding):

    """
    Anonymize top-level Dataframe column (e.g. "city" but not "meta.city")
    for selected users.

    :param df: Dataframe object to operate on
    :param column: column to anonymize
    :param encoding: configured encoding for handling text data

    :return: updated Dataframe object
    """

    # Define udf hash function
    udf_hash_field = udf(lambda x: anonymize_value(x, encoding), df.schema[column].dataType)

    df = df.withColumn(column,
                       when(
                           df['in_source_ids'].isNotNull(),
                           udf_hash_field(col(column))))
    return df


def anonymize_df(df, cfg):

    """
    Core function anonymizing specified Dataframe columns.
    Handles differently top-level fields and nested fields to
    avoid unnecessary JSON conversions

    :param df: Dataframe to anonymize
    :param cfg: ConfigParser object

    :return: update Dataframe object
    """

    # For all target columns, if in_source_ids not null, hash value
    for column in cfg['anonymize']['target_fields'].split(','):
        root_col = column.split('.')[0]

        # If column isn't nested value, avoid JSON serialization
        if column == root_col:
            df = anonymize_top_level_field(df, column, cfg['anonymize']['encoding'])
        else:
            df = anonymize_nested_field(df, column, cfg['anonymize']['encoding'])
    return df


def show_examples(df, cfg, n=5):

    """
    Displays non-null rows for each column anonymized.
    Raises internally-handled EOFError when showing updated nested fields,
    which should be ignored according to spark-user mailing list archives:

    http://mail-archives.apache.org/mod_mbox/spark-user/201604.mbox/
    %3CCA+2Pv=gj0jU25Yn2H65hQHV++6h=L2VttteLQrBLv74O=0xriw
    @mail.gmail.com%3E

    :param df: Spark Dataframe to print
    :param cfg: ConfigParser object
    :param n: number of rows printed
    """

    for c in cfg['anonymize']['target_fields'].split(','):
        df.where(
            col(c).isNotNull()).select(cfg['anonymize']['target_id_key'],
                                           *cfg['anonymize']['target_fields'].split(',')).show(n, truncate=False)


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

    # Read target events JSON data into Dataframe#
    events = sqlContext.read.json(cfg['anonymize']['target_dir'],
                                  multiLine=cfg.getboolean('anonymize', 'multiline'))

    # Left join both Dataframes and flag events to anonymize
    events = df_join(events, user_ids, 'left', cfg)

    # Anonymize events data
    events = anonymize_df(events, cfg)

    show_examples(events, cfg)

