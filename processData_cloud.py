from pyspark.sql import SQLContext
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import when, to_timestamp, split, lpad, concat, collect_list, rank, col, count, isnan, lit, sum
import datetime

#Creating global variables
storage_path = 'gs://practicebucketpiyush/'
current_date = datetime.datetime.now().strftime ("%Y-%m-%d")

#Creating spark context
sc = spark.sparkContext

def check_if_file_exists(filepath):
  testrdd = sc.textFile(filepath).map(lambda line: line.split(","))
  if testrdd.isEmpty():
    return False
  else:
    return True

def read_file(filepath):
  return spark.read.option("header","true").csv(filepath)

#Function for counting not null values
def count_not_null(c, nan_as_null=False):
    pred = col(c).isNotNull() & (~isnan(c) if nan_as_null else lit(True))
    return sum(pred.cast("integer")).alias(c)


#Main Method starts here
if __name__ = "__main__":
  print("################################################################")
  print("Execution Starts here")
  #Read fact files
  fileExists = False
  fileExists = check_if_file_exists(storage_path + "trans_fact_1.csv")
  if fileExists: 
    trans_fact_1 = read_file(storage_path + "trans_fact_1.csv")
    print(current_date + datetime.datetime.now().strftime(' %H:%M:%S') + "trans_fact_1 read")
    fileExists = False
  else: read
    print("trans_fact_1.csv doesn't exist")
  fileExists = check_if_file_exists(storage_path + "trans_fact_2.csv")
  if fileExists: 
    trans_fact_2 = read_file(storage_path + "trans_fact_2.csv")
    print(current_date + datetime.datetime.now().strftime(' %H:%M:%S') + "trans_fact_2 read")
    fileExists = False
  else:
    print("trans_fact_2.csv doesn't exist")
  fileExists = check_if_file_exists(storage_path + "trans_fact_3.csv")
  if fileExists: 
    trans_fact_3 = read_file(storage_path + "trans_fact_3.csv")
    print(current_date + datetime.datetime.now().strftime(' %H:%M:%S') + "trans_fact_3 read")
    fileExists = False
  else:
    print("trans_fact_3.csv doesn't exist")
  fileExists = check_if_file_exists(storage_path + "trans_fact_4.csv")
  if fileExists: 
    trans_fact_4 = read_file(storage_path + "trans_fact_4.csv")
    print(current_date + datetime.datetime.now().strftime(' %H:%M:%S') + "trans_fact_4 read")
    fileExists = False
  else:
    print("trans_fact_4.csv doesn't exist")
  fileExists = check_if_file_exists(storage_path + "trans_fact_5.csv")
  if fileExists: 
    trans_fact_5 = read_file(storage_path + "trans_fact_5.csv")
    print(current_date + datetime.datetime.now().strftime(' %H:%M:%S') + "trans_fact_5 read")
    fileExists = False
  else:
    print("trans_fact_5.csv doesn't exist")
  fileExists = check_if_file_exists(storage_path + "trans_fact_6.csv")
  if fileExists: 
    trans_fact_6 = read_file(storage_path + "trans_fact_6.csv")
    print(current_date + datetime.datetime.now().strftime(' %H:%M:%S') + "trans_fact_6 read")
    fileExists = False
  else:
    print("trans_fact_6.csv doesn't exist")
  fileExists = check_if_file_exists(storage_path + "trans_fact_7.csv")
  if fileExists: 
    trans_fact_7 = read_file(storage_path + "trans_fact_7.csv")
    print(current_date + datetime.datetime.now().strftime(' %H:%M:%S') + "trans_fact_7 read")
    fileExists = False
  else:
    print("trans_fact_7.csv doesn't exist")
  fileExists = check_if_file_exists(storage_path + "trans_fact_8.csv")
  if fileExists: 
    trans_fact_8 = read_file(storage_path + "trans_fact_8.csv")
    print(current_date + datetime.datetime.now().strftime(' %H:%M:%S') + "trans_fact_8 read")
    fileExists = False
  else:
    print("trans_fact_8.csv doesn't exist")
  fileExists = check_if_file_exists(storage_path + "trans_fact_9.csv")
  if fileExists: 
    trans_fact_9 = read_file(storage_path + "trans_fact_9.csv")
    print(current_date + datetime.datetime.now().strftime(' %H:%M:%S') + "trans_fact_9 read")
    fileExists = False
  else:
    print("trans_fact_9.csv doesn't exist")
  fileExists = check_if_file_exists(storage_path + "trans_fact_10.csv")
  if fileExists: 
    trans_fact_10 = read_file(storage_path + "trans_fact_10.csv")
    print(current_date + datetime.datetime.now().strftime(' %H:%M:%S') + "trans_fact_10 read")
    fileExists = False
  else:
    print("trans_fact_10.csv doesn't exist")
  #Reading product file
  fileExists = check_if_file_exists(storage_path + "product.csv")
  if fileExists: 
    product = read_file(storage_path + "product.csv")
    print(current_date + datetime.datetime.now().strftime(' %H:%M:%S') + "product file read")
    fileExists = False
  else:
    print("product.csv doesn't exist")
  #Reading location file
  fileExists = check_if_file_exists(storage_path + "location.csv")
  if fileExists: 
    location = read_file(storage_path + "location.csv")
    print(current_date + datetime.datetime.now().strftime(' %H:%M:%S') + "location file read")
    fileExists = False
  else:
    print("location.csv doesn't exist")

  #Rearranging Columns for all dataframe to combine them later
  trans_fact_2 = trans_fact_2.select(trans_fact_1.columns)
  trans_fact_3 = trans_fact_3.select(trans_fact_1.columns)
  trans_fact_4 = trans_fact_4.select(trans_fact_1.columns)
  trans_fact_5 = trans_fact_5.select(trans_fact_1.columns)

  trans_fact_7 = trans_fact_7.select(trans_fact_6.columns)
  trans_fact_8 = trans_fact_8.select(trans_fact_6.columns)
  trans_fact_9 = trans_fact_9.select(trans_fact_6.columns)
  trans_fact_10 = trans_fact_10.select(trans_fact_6.columns)

  #Combining fact tables 1 to 5 
  trans_fact_grp_1 = trans_fact_1.union(trans_fact_2).union(trans_fact_3).union(trans_fact_4).\
                                       union(trans_fact_5)
  trans_fact_grp_2 = trans_fact_6.union(trans_fact_7).union(trans_fact_8).union(trans_fact_9).\
                                       union(trans_fact_10)
  #renaming trans_id column of grp 2 to macth with grp 1                                     
  trans_fact_grp_2 = trans_fact_grp_2.withColumnRenamed("trans_id", "trans_key")

  #Rearranging columns before merging dataframe
  trans_fact_grp_2 = trans_fact_grp_2.select(trans_fact_grp_1.columns)

  #combining both groups to create a combined trans_fact dataframe
  trans_fact = trans_fact_grp_1.union(trans_fact_grp_2)


  #Combining all dataframes
  df = trans_fact.join(location,trans_fact.store_location_key==location.store_location_key,"left_outer").\
                      drop(location.store_location_key).\
                          join(product,trans_fact.product_key==product.product_key,"left_outer").\
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

  print("Execution Starts here")
  print("################################################################")
  
  #Killing spark context
  sc.stop()



