########################################################################
##
##     Filename : product_category.py
##     Description: This python script contains pyspark code to extract
##                  product categories
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
input_file=config['ExtractProdCategory']['input_file']
output_file=config['ExtractProdCategory']['output_file']


def extract_product_category():
    # Creating the spark session

    spark = SparkSession \
        .builder \
        .appName("Product Category Read") \
        .config("spark.driver.extraClassPath",spark_driver_extraClassPath) \
        .config("spark.executor.extraClassPath",spark_executor_extraClassPath) \
        .getOrCreate()

    # sc = spark.sparkContext

    # Source data file path -- metadata.json
    path = input_file

    # List of columns except salesRank
    colList=['categories']

    # Product metadata dataframe object creation
    productDF = spark.read.json(path,allowBackslashEscapingAnyCharacter=True)

    # Filter the corrupted data
    correctProdCategoryDF=productDF.filter(productDF['asin'] != 'null').select(*colList)

    # Final product dataframe with category values exploded
    finalProdDF_1=correctProdCategoryDF.select(*colList, F.explode('categories').alias('lvl1_cat'))
    finalProdDF_2=finalProdDF_1.select(*colList, F.explode('lvl1_cat').alias('lvl2_cat'))
    finalProdDF_3=finalProdDF_2.select('lvl2_cat').distinct()
    finalProdDF_4=finalProdDF_3.withColumnRenamed('lvl2_cat','category')

    ##############################################
    # Writing data into parquet format file
    ##############################################

    # Output path
    output_file_path=output_file


    # Saving the data into output file
    finalProdDF_4.write.mode('overwrite').parquet(output_file_path)

    # End of the script
    print('Ended Successfully')

if __name__=='__main__':
    extract_product_category()