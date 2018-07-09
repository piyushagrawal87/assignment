#from pytest_spark import spark_context
from pyspark.sql import SQLContext
from processData_cloud import read_file

def test_2(spark_context):
    sqlContext = SQLContext(spark_context)
    file = read_file('test.txt',sqlContext)
    assert file.rdd.isEmpty() == False

