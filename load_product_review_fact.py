########################################################################
##
##     Filename : product_review.py
##     Description: This python script contains pyspark code to load
##                  product review fact from parquet data
##
##     Created By : Som Debnath
##     Creation Date: 27-Feb-2021
##
##########################################################################
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pandas as pd
from itertools import chain
import os
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

input_file=config['LoadProductReviewFact']['input_file']



def load_product_review_fact(run_id):
    # Creating the spark session
    spark = SparkSession \
        .builder \
        .appName("Product review data extraction and load") \
        .config("spark.driver.extraClassPath", spark_driver_extraClassPath) \
        .config("spark.executor.extraClassPath", spark_executor_extraClassPath) \
        .getOrCreate()

    # Source data file path --review parquet
    path = "C:\\Users\\somde\\takeaway\\product_review.parquet\\*"

    # Product review dataframe object creation
    reviewDF = spark.read.parquet(path)

    # Create temp view on review data
    reviewDF.createOrReplaceTempView("reviewDF")

    # Reading active product data from DWH for lookup
    productDF = spark.read \
        .format(conn_format) \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("driver",driver)\
        .option("query","SELECT product_key,asin FROM amazon_review.d_product WHERE active_flg='Y'")\
        .load()

    # Creating temp view on product Data
    productDF.createOrReplaceTempView("productDF")

    # Preparing review fact data for loading into DWH
    reviewFactDF=spark.sql("SELECT\
                                    A.reviewerID as reviewer_id,\
                                    nvl(B.product_key,-1) as product_key,\
                                    A.reviewerName as reviewer_name,\
                                    A.helpful_val as helpful,\
                                    A.reviewText as review_text,\
                                    A.overall as overall_rating,\
                                    A.summary as review_summary,\
                                    A.unixReviewTime as unix_review_time,\
                                    cast(date_format(to_date(reviewTime,'MM d, y'),'yMMdd') as int) date_key \
                            FROM reviewDF A LEFT JOIN productDF B \
                           ON (A.asin=B.asin)")

    reviewFactWithRunIdDF=reviewFactDF.withColumn("run_id",F.lit(run_id).cast("int"))

    # Loading data into temp table in DWH for further processing
    reviewFactWithRunIdDF.select( \
                            'reviewer_id',
                            'product_key',
                            'reviewer_name',
                            'helpful',
                            'review_text',
                            'overall_rating',
                            'review_summary',
                            'unix_review_time',
                            'date_key',
                            'run_id'
                        ).write \
                        .format(conn_format) \
                        .option("url", url) \
                        .option("dbtable", "amazon_review.f_product_review") \
                        .option("user", user) \
                        .option("password", password) \
                        .option("driver", driver) \
                        .mode("append") \
                        .save()

if __name__ == '__main__':
    load_product_review_fact(sys.argv[1])