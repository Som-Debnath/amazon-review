#############################################################
##
##   Name : data_presentation.py
##   Description: Create some insights about the data
##   Created By: Som Debnath
##   Creation Date: 3-March-2021
##
#############################################################

import pandas as pd
import numpy as np
import seaborn as sns
from matplotlib import pyplot as plt
import database as db
from pyspark.sql import SparkSession
import configparser

# Configuration Parameters

config = configparser.ConfigParser()
config.read('config.ini')
spark_driver_extraClassPath=config['SPARK']['spark.driver.extraClassPath']
spark_executor_extraClassPath=config['SPARK']['spark.executor.extraClassPath']
product_category_report_file=config['PRSENTATION']['product_category_report_file']
prod_with_pricebucket_report_file=config['PRSENTATION']['prod_with_pricebucket_report_file']
prod_review_hist_file=config['PRSENTATION']['prod_review_hist_file']

conn_format=config['DATABASE']['format']
url=config['DATABASE']['url']
user=config['DATABASE']['user']
password=config['DATABASE']['password']
driver=config['DATABASE']['driver']


# Get the data from Source Database

def data_presentation():
    # Creating the spark session
    spark = SparkSession \
        .builder \
        .appName("Presentation layer") \
        .config("spark.driver.extraClassPath",spark_driver_extraClassPath) \
        .config("spark.executor.extraClassPath",spark_executor_extraClassPath) \
        .getOrCreate()

    # Reading review count per product
    productReviewCountDF = spark.read \
        .format(conn_format) \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .option("query", "SELECT \
                                   count(*) as count,\
                                   B.asin as product_id \
                           FROM amazon_review.f_product_review A, amazon_review.d_product B\
                           WHERE A.product_key=B.product_key\
                           group by B.asin,B.product_key\
                           having count(*)>30"
                )\
        .load()
    if productReviewCountDF.count()>0:
        sns.set(rc={'figure.figsize': (25, 20)})
        reviewHistDF=productReviewCountDF.toPandas()
        fig=sns.barplot(data=reviewHistDF, x="product_id",y="count")
        fig.set(xlabel='Product Id', ylabel='Review Count')
        plt.setp(fig.get_xticklabels(), rotation=45)
        fig.get_figure().savefig(prod_review_hist_file)
    else:
        print('productReviewCountDF dataframe does not data')

    # Reading product count per product price bucket
    productWithPriceBucketDF = spark.read \
        .format(conn_format) \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .option("query", "SELECT \
                                       count(*) as count,\
                                       B.price_bucket_desc as bucket_name \
                               FROM amazon_review.d_product A, amazon_review.d_price_bucket B\
                               WHERE A.price_bucket_key=B.price_bucket_key\
                               group by B.price_bucket_desc"
                ) \
        .load()
    if productWithPriceBucketDF.count()>0:
        sns.set(rc={'figure.figsize': (40, 35)})
        productWithPriceBucketDF = productWithPriceBucketDF.toPandas()
        fig = sns.barplot(data=productWithPriceBucketDF, x="bucket_name", y="count")
        fig.set(xlabel='Price bucket', ylabel='Product Count')
        plt.setp(fig.get_xticklabels(), rotation=45)
        fig.get_figure().savefig(prod_with_pricebucket_report_file)
    else:
        print('productWithPriceBucketDF dataframe does not have any data')

    # Reading product count per product category
    productWithCategoryDF = spark.read \
        .format(conn_format) \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .option("query", "SELECT \
                                           count(*) as count,\
                                           B.category_name \
                                   FROM amazon_review.d_product A, amazon_review.d_product_category B\
                                   WHERE A.prod_category_key=B.prod_category_key\
                                   group by B.category_name"
                ) \
        .load()
    if productWithCategoryDF.count() > 0:
        sns.set(rc={'figure.figsize': (40, 35)})
        productWithCategoryDF = productWithCategoryDF.toPandas()
        fig = sns.barplot(data=productWithCategoryDF, x="category_name", y="count")
        fig.set(xlabel='Product Category', ylabel='Product Count')
        plt.setp(fig.get_xticklabels(), rotation=45)
        fig.get_figure().savefig(product_category_report_file)
    else:
        print('productWithCategoryDF dataframe does not have data')


if __name__ == '__main__':
    data_presentation()