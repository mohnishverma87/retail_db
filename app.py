# Load the runtime properties
import pyspark
import sys
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import configparser as cp

from util import get_tables
def main():
    tables = get_tables('Table_list')
    # for records in tables['table_name']:
    # print(records)

if __name__ == '__main__':
    main()

props = cp.RawConfigParser()
props.read("config.py")

# Read the data from Source
sc = SparkContext()
spark =SparkSession.builder.\
    appName("DataPipelineProject").\
    master('local').getOrCreate()
spark.conf.set('spark.sql.shuffle.partitions','2')
SOURCE_PATH=props.get('prd','source')

orderDF = spark.read.csv(SOURCE_PATH+"orders", sep = ',', \
            schema='order_id int, order_date timestamp, order_customer_id int, order_status string')
orders = orderDF.createOrReplaceTempView("orders")
print(spark.sql("SELECT * FROM ORDERS LIMIT 5").collect())


#Lets See..
# Process the data using Pandas
# Write the data to Target