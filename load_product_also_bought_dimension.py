#########################################################################
##
##     Filename : load_product_also_bought_dimension.py
##     Description: This python script contains pyspark code to load
##                  product also bought dimension in DWH
##
##     Created By : Som Debnath
##     Creation Date: 28-Feb-2021
##
##########################################################################

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pandas as pd
from itertools import chain
import os
import database as db
import sys
import configparser

# Configuration Parameters

config = configparser.ConfigParser()
config.read('config.ini')

spark_driver_extraClassPath=config['SPARK']['spark.driver.extraClassPath']
spark_executor_extraClassPath=config['SPARK']['spark.executor.extraClassPath']

conn_format=config['DATABASE']['format']
url=config['DATABASE']['url']
user=config['DATABASE']['user']
password=config['DATABASE']['password']
driver=config['DATABASE']['driver']

input_file=config['LoadProdAlsoBoughtDimension']['input_file']


def load_product_also_bought_dimension(run_id):
    # Creating the spark session

    spark = SparkSession \
        .builder \
        .appName("Loading product also bought dimension") \
        .config("spark.driver.extraClassPath",spark_driver_extraClassPath) \
        .config("spark.executor.extraClassPath",spark_executor_extraClassPath) \
        .getOrCreate()

    # Source data file path
    path = input_file

    # Read product also bought parquet files
    productAlsoBoughtDF = spark.read.parquet(path)

    # Loading data into temp table in DWH for further processing
    productAlsoBoughtDF.select('asin','also_bought').write \
                              .format(conn_format) \
                              .option("url", url) \
                              .option("dbtable", "amazon_review.temp_product_also_bought") \
                              .option("user", user) \
                              .option("password", password) \
                              .option("driver", driver) \
                              .mode("overwrite")\
                              .save()
    # Calling the stored proc to load d_product_also_bought
    conn = db.connection()
    cur=conn.cursor()
    cur.execute('call amazon_review.proc_load_product_also_bought(%s)',(run_id,))
    conn.commit()
    db.close(conn,cur)

if __name__ == '__main__':
    load_product_also_bought_dimension(sys.argv[1])