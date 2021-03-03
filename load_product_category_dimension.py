########################################################################
##
##     Filename : load_product_category_dimension.py
##     Description: This python script contains pyspark code to load
##                  production category dimension in DWH
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
import sys
import database as db
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

input_file=config['LoadProductCategoryDimension']['input_file']

# Creating the spark session

def load_product_category(run_id):
    spark = SparkSession \
        .builder \
        .appName("Loading Product Category Dimension") \
        .config("spark.driver.extraClassPath",spark_driver_extraClassPath) \
        .config("spark.executor.extraClassPath",spark_executor_extraClassPath) \
        .getOrCreate()

    # sc = spark.sparkContext

    # Source data file path
    path = input_file

    # Read product category parquet files for product information
    productCatDF = spark.read.parquet(path)

    # Creating temp views on source DataFrame
    productCatDF.createOrReplaceTempView("productCatDF")

    finalproductCatDF=spark.sql("SELECT category as category_name\
                                                FROM productCatDF")
    finalproductCatDF.select('category_name').write \
        .format(conn_format) \
        .option("url", url) \
        .option("dbtable", "amazon_review.temp_product_category") \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .mode("overwrite")\
        .save()
    print('runid ->',run_id)
    # Calling the stored proc to load d_product_dimension
    conn = db.connection()
    cur = conn.cursor()
    cur.execute('call amazon_review.proc_load_product_category_dimension(%s)', (run_id,))
    conn.commit()
    db.close(conn, cur)

if __name__ == '__main__':
    load_product_category(sys.argv[1])
