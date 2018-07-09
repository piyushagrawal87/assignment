#from pytest_spark import spark_context
from processData_cloud import check_if_file_exists

def test_2(spark_context):
    blnFileExists = check_if_file_exists('hello_world.py',spark_context)
    assert blnFileExists == True

