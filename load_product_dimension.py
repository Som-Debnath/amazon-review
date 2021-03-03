########################################################################
##
##     Filename : load_product_dimension.py
##     Description: This python script contains pyspark code to load
##                  production dimension in DWH
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

input_file=config['LoadProductDimension']['input_file']

def load_product_dimension(run_id):
    # Creating the spark session

    spark = SparkSession \
        .builder \
        .appName("Loading Product Dimension") \
        .config("spark.driver.extraClassPath",spark_driver_extraClassPath) \
        .config("spark.executor.extraClassPath",spark_executor_extraClassPath) \
        .getOrCreate()

    # Source data file path
    path = input_file


    # Read product parquet files for product information
    productDF = spark.read.parquet(path)

    print(productDF.count())

    # Reading active product category data from DWH for lookup
    productCategoryDF = spark.read \
        .format(conn_format) \
        .option("url", url) \
        .option("query","select prod_category_key,category_name from amazon_review.d_product_category")\
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver)\
        .load()


    # Reading active price bucket data from DWH for lookup
    productPriceBucketDF= spark.read \
        .format(conn_format) \
        .option("url", url) \
        .option("query", "select price_bucket_key,bucket_lower_val,bucket_higher_val from amazon_review.d_price_bucket where active_flg ='Y'") \
        .option("user", user) \
        .option("password", password) \
        .option("driver",driver)\
        .load()

    # Creating temp views on source product DataFrame
    productDF.createOrReplaceTempView("productDF")

    # Creating temp views on Product Category Dataframe
    productCategoryDF.createOrReplaceTempView("productCategoryDF")

    #Creating temp views on Price Bucket Dataframe
    productPriceBucketDF.createOrReplaceTempView("productPriceBucketDF")

    # Looking up the Product Category Key for each product
    finalProductDimensionWithProdCategoryDF=spark.sql("SELECT\
                                                            A.asin,\
                                                            A.brand,\
                                                            A.description as product_desc,\
                                                            A.imUrl,\
                                                            A.price,\
                                                            A.title as product_title,\
                                                            A.sales_rank_cat,\
                                                            A.sales_rank_val as sales_rank,\
                                                            nvl(B.prod_category_key,-1) as prod_category_key\
                                                            FROM productDF AS A left join productCategoryDF AS B\
                                                            ON A.highest_category=B.category_name\
                                                            ")

    print(finalProductDimensionWithProdCategoryDF.count())

    # Creating temp views on the product dataframe contains product category key
    finalProductDimensionWithProdCategoryDF.createOrReplaceTempView("finalProductDimensionWithProdCategoryDF")


    # Lookup price bucket dimension for bucket key
    finalProductDimensionWithPriceBucketDF=spark.sql("SELECT \
                                                            A.asin,\
                                                            A.brand,\
                                                            A.product_desc,\
                                                            A.imUrl,\
                                                            nvl(A.price,-1) as price,\
                                                            A.product_title,\
                                                            A.sales_rank_cat,\
                                                            A.sales_rank,\
                                                            A.prod_category_key,\
                                                            B.price_bucket_key\
                                                       FROM  finalProductDimensionWithProdCategoryDF AS A, productPriceBucketDF AS B\
                                                       WHERE nvl(A.price,-1) >= B.bucket_lower_val\
                                                         AND nvl(A.price,-1) <= B.bucket_higher_val\
                                                       ")

    print(finalProductDimensionWithPriceBucketDF.count())

    # Creating temp views on product data contains category key and bucket key
    finalProductDimensionWithPriceBucketDF.createOrReplaceTempView("finalProductDimensionWithPriceBucketDF")

    # Lookup product category dimension for sales rank categroy key (Role playing)
    finalProductDimensionWithSalesRankCatKeyDF=spark.sql("SELECT \
                                                            A.asin,\
                                                            A.brand,\
                                                            A.product_desc,\
                                                            A.imUrl,\
                                                            A.price,\
                                                            A.product_title,\
                                                            A.sales_rank_cat,\
                                                            A.sales_rank,\
                                                            A.prod_category_key,\
                                                            A.price_bucket_key,\
                                                            nvl(B.prod_category_key,-1) as sales_rank_cat_key\
                                                       FROM  finalProductDimensionWithPriceBucketDF AS A left join productCategoryDF B\
                                                         ON A.sales_rank_cat=B.category_name\
                                                       ")

    print(finalProductDimensionWithSalesRankCatKeyDF.count())

    # Write data into DWH environment (PostGreSql)
    finalProductDimensionWithSalesRankCatKeyDF.select(\
                                                      'asin','product_title','product_desc',\
                                                      'brand','price','sales_rank','sales_rank_cat_key',\
                                                      'prod_category_key','price_bucket_key').write \
                                                .format(conn_format) \
                                                .option("url", url) \
                                                .option("dbtable", "amazon_review.temp_product") \
                                                .option("user", user) \
                                                .option("password", password) \
                                                .option("driver", driver) \
                                                .mode("overwrite")\
                                                .save()

    # Calling the stored proc to load d_product_dimension
    conn = db.connection()
    cur = conn.cursor()
    cur.execute('call amazon_review.proc_load_product_dimension(%s)', (run_id,))
    conn.commit()
    db.close(conn, cur)
if __name__=='__main__':
    load_product_dimension(sys.argv[1])