########################################################################
##
##     Filename : related_product.py
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
input_file=config['ExtractRelatedProduct']['input_file']
output_file_related_also_bought_product=config['ExtractRelatedProduct']['output_file_related_also_bought_product']
output_file_related_also_viewed_product=config['ExtractRelatedProduct']['output_file_related_also_viewed_product']
output_file_related_bought_together_product=config['ExtractRelatedProduct']['output_file_related_bought_together_product']
output_file_related_buy_after_viewing_product=config['ExtractRelatedProduct']['output_file_related_buy_after_viewing_product']

def extract_related_product():
    # Creating the spark session

    spark = SparkSession \
            .builder \
            .appName("Python Spark Related Product Data Load") \
            .config("spark.driver.extraClassPath",spark_driver_extraClassPath) \
            .config("spark.executor.extraClassPath",spark_executor_extraClassPath) \
            .getOrCreate()

    # sc = spark.sparkContext

    # Source data file path -- metadata.json
    path = input_file

    # Product metadata dataframe object creation
    productDF = spark.read.json(path, allowBackslashEscapingAnyCharacter=True).limit(1000)

    # Filter the corrupted data
    correctRelatedProdDF = productDF.filter(productDF['asin'] != 'null').select('asin','related.*')

    # Distinct product and related also bought
    relatedAlsoBoughtDF=correctRelatedProdDF.select('asin',F.explode('also_bought').alias('also_bought')).distinct()


    # Distinct product and related also viewed
    relatedAlsoViewedDF=correctRelatedProdDF.select('asin',F.explode('also_viewed').alias('also_viewed')).distinct()


    # Distinct product and related bought together
    relatedBoughtTogetherDF=correctRelatedProdDF.select('asin',F.explode('bought_together').alias('bought_together')).distinct()


    # Distinct product and related buy_after_viewing
    relatedBuyAfterViewingDF=correctRelatedProdDF.select('asin',F.explode('buy_after_viewing').alias('buy_after_viewing')).distinct()

    ##############################################
    # Writing data into parquet format file
    ##############################################
    # Output filename
    output_file_1=output_file_related_also_bought_product
    output_file_2=output_file_related_also_viewed_product
    output_file_3=output_file_related_bought_together_product
    output_file_4=output_file_related_buy_after_viewing_product

    # Saving the data into output file
    relatedAlsoBoughtDF.write.mode('overwrite').parquet(output_file_1)
    relatedAlsoViewedDF.write.mode('overwrite').parquet(output_file_2)
    relatedBoughtTogetherDF.write.mode('overwrite').parquet(output_file_3)
    relatedBuyAfterViewingDF.write.mode('overwrite').parquet(output_file_4)

    # End of the script
    print('Ended Successfully')
if __name__ == '__main__':
    extract_related_product()
