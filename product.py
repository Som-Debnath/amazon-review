########################################################################
##
##     Filename : product.py
##     Description: This python script contains pyspark code to read the
##                  product metadata (json)
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
input_file=config['ExtractProduct']['input_file']
output_file=config['ExtractProduct']['output_file']

def extract_product():
    # Creating the spark session

    spark = SparkSession \
        .builder \
        .appName("Python Spark Product Data Load") \
        .config("spark.driver.extraClassPath",spark_driver_extraClassPath) \
        .config("spark.executor.extraClassPath",spark_driver_extraClassPath) \
        .getOrCreate()

    # sc = spark.sparkContext

    # Source data file path -- metadata.json
    path = input_file

    # List of columns except salesRank
    colList=['asin','brand','description','imUrl','price','title','categories']
    finalColList=['asin','brand','description','imUrl','price','title',"sales_rank_cat","sales_rank_val","highest_category"]

    # Product metadata dataframe object creation
    productDF = spark.read.json(path,allowBackslashEscapingAnyCharacter=True).limit(10000)

    # Filter the corrupted data
    correctProdDF=productDF.filter(productDF['asin'] != 'null').select(*colList,'salesRank.*')

    # The struct salesRank field is converted column mapping
    explodedSalesRanK = F.create_map(list(chain(*((F.lit(name), F.col(name)) for name in correctProdDF.columns if name not in colList)))).alias("explodedSalesRanK")

    # Distinct Product information where salesRank IS NOT NULL
    finalProdWithoutNullSalesRankDF=correctProdDF.select(*colList, F.explode(explodedSalesRanK).\
                                     alias("sales_rank_cat","sales_rank_val")).\
                                     where(F.col('sales_rank_val').isNotNull()).dropDuplicates(['asin'])

    # Distinct Product information where  salesRank IS  NULL
    finalProdWithNullSalesRankDistinctDF=correctProdDF.select(*colList, F.explode(explodedSalesRanK).\
                                     alias("sales_rank_cat","sales_rank_val")).\
                                     where(F.col('sales_rank_val').isNull()).dropDuplicates(['asin'])

    # Creating temp views on finalProdWithoutNullSalesRankDF
    finalProdWithoutNullSalesRankDF.createOrReplaceTempView("finalProdWithoutNullSalesRankDF")

    # Creating temp views on finalProdWithNullSalesRankDistinctDF
    finalProdWithNullSalesRankDistinctDF.createOrReplaceTempView("finalProdWithNullSalesRankDistinctDF")

    # Select distinct products where all the salesrank is null (after explode)
    finalProdWithNullSalesRankDF = spark.sql("select \
                                                    finalProdWithNullSalesRankDistinctDF.asin,\
                                                    finalProdWithNullSalesRankDistinctDF.brand,\
                                                    finalProdWithNullSalesRankDistinctDF.description,\
                                                    finalProdWithNullSalesRankDistinctDF.imUrl,\
                                                    finalProdWithNullSalesRankDistinctDF.price,\
                                                    finalProdWithNullSalesRankDistinctDF.title,\
                                                    finalProdWithNullSalesRankDistinctDF.categories,\
                                                    finalProdWithNullSalesRankDistinctDF.sales_rank_cat,\
                                                    finalProdWithNullSalesRankDistinctDF.sales_rank_val \
                                             from   finalProdWithNullSalesRankDistinctDF LEFT JOIN finalProdWithoutNullSalesRankDF\
                                                 ON finalProdWithNullSalesRankDistinctDF.asin=finalProdWithoutNullSalesRankDF.asin\
                                                 WHERE finalProdWithoutNullSalesRankDF.asin is null")

    # Union the dataframes for distinct products having all types of sales rank (null & not null)
    finalProdDF=finalProdWithoutNullSalesRankDF.union(finalProdWithNullSalesRankDF)

    # Dataframe having highest category mapped
    finalProdWithHighestCategory=finalProdDF.select(*colList,"sales_rank_cat","sales_rank_val",finalProdDF['categories'][0][0].alias('highest_category')).drop('categories')

    # Dataframe with distinct values
    finalProdWithDistinctValDF=finalProdWithHighestCategory.select(*finalColList)


    ##############################################
    # Writing data into parquet format file
    ##############################################

    # Output path
    output_path_file=output_file

    # Saving the data into output file
    finalProdWithDistinctValDF.write.mode('overwrite').parquet(output_path_file)

    # End of the script
    print('Ended Successfully')

if __name__=='__main__':
    extract_product()