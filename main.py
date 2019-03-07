import pyspark

# Script parameters to move to configuration file
TEST_DIR = './test_data/*'
MULTILINE = False
TARGET_UUIDS = ['f8ae431d23c1af07b7010a20b96bbf62', '856d3a356a293db4ace7bf4ba0a90e6f']
TARGET_FIELDS = ['email', 'country', 'meta.city', 'meta.ipv4']

if __name__ == '__main__':

    # Creating Spark SQLContext
    sc = pyspark.SparkContext()
    sqlContext = pyspark.SQLContext(sc)

    # Reading json data into a dataframe
    df = sqlContext.read.json(TEST_DIR, multiLine=MULTILINE)

    df.select('uuid').show(50, truncate=False)
    df.printSchema()
