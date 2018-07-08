from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import when, to_timestamp, split, lpad, concat, collect_list, rank, col, count, isnan, lit, sum
import datetime
from subprocess import Popen, PIPE
import re

#Creating global variables
bucket_name = 'practicebucketpiyush'
storage_path = 'gs://' + bucket_name + "/"
current_date = datetime.datetime.now().strftime ("%Y-%m-%d")

def check_if_file_exists(filepath, sparkcontext):
  testrdd = sparkcontext.textFile(filepath).map(lambda line: line.split(","))
  if testrdd.isEmpty():
    return False
  else:
    return True

def read_file(filepath):
  return sqlContext.read.option("header","true").csv(filepath)

#Function for counting not null values
def count_not_null(c, nan_as_null=False):
    pred = col(c).isNotNull() & (~isnan(c) if nan_as_null else lit(True))
    return sum(pred.cast("integer")).alias(c)

#Main Method starts here
if __name__ == "__main__":

  print("################################################################")
  print("Execution Starts here")

  #Creating spark context
  sc = SparkContext()
  sqlContext = SQLContext(sc)

  #Creating an empty dataframe for loading fact file
  schema = StructType([StructField("field1", StringType(), True),
                      StructField("field2", StringType(), True),
                      StructField("field3", StringType(), True),
                      StructField("field4", StringType(), True),
                      StructField("field5", StringType(), True),
                      StructField("field6", StringType(), True),
                      StructField("field7", StringType(), True)])

  df = sqlContext.createDataFrame(sc.emptyRDD(), schema)

  #Gathering File names from the input bucket
  cmd = ('hdfs dfs -ls gs://' + bucket_name).split()
  proc = Popen(cmd, stdout=PIPE)
  filenames = proc.communicate()[0].decode().split('\n')
  filenames = [s for s in filenames if bucket_name in s]
  files = [re.search(bucket_name + '/(.+)', file).group(1) for file in filenames]

  for file in files:
    if 'location' in file:
      location = read_file(storage_path + file)
    elif 'product' in file:
      product = read_file(storage_path + file)
    else:
      temp_df = read_file(storage_path + file)
      if 'store_location_key' in temp_df.columns[0]:
        df = temp_df.union(df)
      if 'trans_id' in temp_df.columns[6]:
        temp_df = read_file(storage_path + file)
        temp_df = temp_df.withColumnRenamed("trans_id", "trans_key")
        temp_df = temp_df.select(df.columns)
        df = temp_df.union(df)

  #Combining all dataframes
  df = df.join(location,df.store_location_key==location.store_location_key,"left_outer").\
                      drop(location.store_location_key).\
                          join(product,df.product_key==product.product_key,"left_outer").\
                              drop(product.product_key)

  #Filling NAs in dataframe for units and sales with 0
  df = df.na.fill({'units':0,'sales':0})

  #Identifying Non Loyalty and Loyalty Customers
  df = df.withColumn("customer_type",when(col("collector_key") < 0, lit('NonLoyalty')).otherwise(lit('Loyalty')))

  #Bring df to cache
  df.cache()

  #%%%%%%%%%%%Total Sales by cutomer type%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
  totals = df.filter(df.customer_type == 'Loyalty').groupby('customer_type').agg({'sales':'sum'}).withColumnRenamed("sum(sales)", "total")
  totals = totals.withColumn('measure', lit('total sales'))
  temp = df.filter(df.customer_type == 'NonLoyalty').groupby('customer_type').agg({'sales':'sum'}).withColumnRenamed("sum(sales)", "total")
  temp = temp.withColumn('measure', lit('total sales'))
  totals = totals.union(temp)
  #%%%%%%%%%%%Total Units by cutomer type%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
  temp = df.filter(df.customer_type == 'Loyalty').groupby('customer_type').agg({'units':'sum'}).withColumnRenamed("sum(units)", "total")
  temp = temp.withColumn('measure', lit('total units'))
  totals = totals.union(temp)
  temp = df.filter(df.customer_type == 'NonLoyalty').groupby('customer_type').agg({'units':'sum'}).withColumnRenamed("sum(units)", "total")
  temp = temp.withColumn('measure', lit('total units'))
  totals = totals.union(temp)
  #%%%%%%%%%%%Distinct transaction Counts by cutomer type%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
  temp = df.filter(df.customer_type == 'Loyalty').select('trans_key').distinct().agg({'trans_key':'count'}).withColumnRenamed("count(trans_key)", "total")
  temp = temp.withColumn('measure', lit('total distinct trasactions'))
  temp = temp.withColumn('customer_type', lit('Loyalty'))
  totals = totals.union(temp.select('customer_type','total','measure'))
  temp = df.filter(df.customer_type == 'NonLoyalty').select('trans_key').distinct().agg({'trans_key':'count'}).withColumnRenamed("count(trans_key)", "total")
  temp = temp.withColumn('measure', lit('total distinct trasactions'))
  temp = temp.withColumn('customer_type', lit('NonLoyalty'))
  totals = totals.union(temp.select('customer_type','total','measure'))
  #%%%%%%%%%%%Distinct Collector Counts by cutomer type%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
  temp = df.filter(df.customer_type == 'Loyalty').select('collector_key').distinct().agg({'collector_key':'count'}).withColumnRenamed("count(collector_key)", "total")
  temp = temp.withColumn('measure', lit('total distinct collectors'))
  temp = temp.withColumn('customer_type', lit('Loyalty'))
  totals = totals.union(temp.select('customer_type','total','measure'))
  temp = df.filter(df.customer_type == 'NonLoyalty').select('collector_key').distinct().agg({'collector_key':'count'}).withColumnRenamed("count(collector_key)", "total")
  temp = temp.withColumn('measure', lit('total distinct collectors'))
  temp = temp.withColumn('customer_type', lit('NonLoyalty'))
  totals = totals.union(temp.select('customer_type','total','measure'))
  #%%%%%%%%%%Saving the data in the database%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
  totals.write.format('jdbc').options(
            url='jdbc:mysql://35.238.212.81:3306/assignment_db',
            driver='com.mysql.jdbc.Driver',
            dbtable='totals',
            user='assignment_user',
            password='Pa$$word').mode('overwrite').save()
  #Making transaction date consistent (/,-, consistent) and replacing day part of the trans_dt to 01 to calculate monthly aggregate
  df_trans_date_slash = df.filter(df.trans_dt.contains('/'))
  split_col = split(df_trans_date_slash['trans_dt'], '/')
  df_trans_date_slash = df_trans_date_slash.withColumn('month', split_col.getItem(0))
  df_trans_date_slash = df_trans_date_slash.withColumn('month', lpad('month',2,"0"))
  df_trans_date_slash = df_trans_date_slash.withColumn('year', split_col.getItem(2))
  df_trans_date_slash = df_trans_date_slash.withColumn('trans_dt', concat(col('month'),lit('-01-'),col('year')))
  df_trans_date_hyphen = df.filter(df.trans_dt.contains('-'))
  split_col = split(df_trans_date_hyphen['trans_dt'], '-')
  df_trans_date_hyphen = df_trans_date_hyphen.withColumn('month', split_col.getItem(1))
  df_trans_date_hyphen = df_trans_date_hyphen.withColumn('month', lpad('month',2,"0"))
  df_trans_date_hyphen = df_trans_date_hyphen.withColumn('year', split_col.getItem(0))
  df_trans_date_hyphen = df_trans_date_hyphen.withColumn('trans_dt', concat(col('month'),lit('-01-'),col('year')))
  df = df_trans_date_hyphen.union(df_trans_date_slash)
  #Monthly trends
  trends = df.groupby('customer_type','trans_dt').agg({'sales':'sum'}).withColumnRenamed("sum(sales)", "total").orderBy('customer_type','trans_dt')
  trends = trends.withColumn('measure', lit('total sales'))
  temp = df.groupby('customer_type','trans_dt').agg({'units':'sum'}).withColumnRenamed("sum(units)", "total").orderBy('customer_type','trans_dt')
  temp = temp.withColumn('measure', lit('total units'))
  trends = trends.union(temp)
  temp = df.groupby('customer_type','trans_dt','trans_key').agg(collect_list('trans_key').getItem(0).alias('trans_key_temp'))
  temp = temp.groupby('customer_type','trans_dt').agg({'trans_key':'count'}).withColumnRenamed("count(trans_key)", "total")
  temp = temp.withColumn('measure', lit('total distinct trans'))
  trends = trends.union(temp)
  temp = df.groupby('customer_type','trans_dt','collector_key').agg(collect_list('collector_key').getItem(0).alias('collector_key_temp'))
  temp = temp.groupby('customer_type','trans_dt').agg({'collector_key':'count'}).withColumnRenamed("count(collector_key)", "total")
  temp = temp.withColumn('measure', lit('total distinct collectors'))
  trends = trends.union(temp)
  #%%%%%%%%%%Saving the data in the database
  trends.write.format('jdbc').options(
            url='jdbc:mysql://35.238.212.81:3306/assignment_db',
            driver='com.mysql.jdbc.Driver',
            dbtable='trends',
            user='assignment_user',
            password='Pa$$word').mode('overwrite').save()

  #Uncache df
  df.unpersist()

  print("Execution ends here")
  print("################################################################")

  #Killing spark context
  sc.stop()
