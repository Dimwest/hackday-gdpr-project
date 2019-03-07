import pyspark
from pyspark.sql.functions import udf

TEST_DIR = './test_data/*'

sc = pyspark.SparkContext()
sqlContext = pyspark.SQLContext(sc)

# Reading json data into a dataframe
df = sqlContext.read.json(TEST_DIR)

df.show(5)
