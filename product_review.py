########################################################################
##
##     Filename : product_review.py
##     Description: This python script contains pyspark code to extract
##                  also viewed related product
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
import configparser

# Configuration Parameters

config = configparser.ConfigParser()
config.read('config.ini')
spark_driver_extraClassPath=config['SPARK']['spark.driver.extraClassPath']
spark_executor_extraClassPath=config['SPARK']['spark.executor.extraClassPath']
input_file=config['ExtractProductReview']['input_file']
output_file=config['ExtractProductReview']['output_file']


def extract_product_review():
    # Creating the spark session
    spark = SparkSession \
        .builder \
        .appName("Product review data extraction and load") \
        .config("spark.driver.extraClassPath",spark_driver_extraClassPath) \
        .config("spark.executor.extraClassPath",spark_executor_extraClassPath) \
        .getOrCreate()


    # Source data file path -- metadata.json
    path = input_file

    # Product review dataframe object creation
    reviewDF = spark.read.json(path, allowBackslashEscapingAnyCharacter=True).limit(10000)
    columnList=reviewDF.columns
    reviewDF_1=reviewDF.select(*columnList,F.concat(reviewDF['helpful'][0],F.lit("/"),reviewDF['helpful'][1]).alias('helpful_val'))
    reviewDF_2=reviewDF_1.drop('helpful')

    reviewDF_2.show()
    print(reviewDF_2.count())


    # De-duplicating the data
    reviewDeDupDF=reviewDF_2.dropDuplicates(['asin','reviewerID','unixReviewTime'])

    ##############################################
    # Writing data into parquet format file
    ##############################################

    # Output path
    output_file_path=output_file
    
    # Saving the data into output file
    reviewDeDupDF.write.mode('overwrite').parquet(output_file_path)
    
    # End of the script
    print('Ended Successfully')

if __name__ == '__main__':
    extract_product_review()