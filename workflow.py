#######################################################################
##
##    Name: workflow.py
##    Description: Python script to build workflow or orchestrate
##                 using luigi
##    Created By : Som Debnath
##    Creation Date: 2-March-2021
##
######################################################################
import configparser
import os
import datetime
import luigi

import database as db

import source_data_download as sdd
import product as prod
import product_category as p_cat
import related_products as rel_prod
import product_review as p_review

import load_product_category_dimension as p_cat_dim
import load_product_dimension as p_dim
import load_product_also_bought_dimension as p_also_bought_dim
import load_product_also_viewed_dimension as p_also_viewed_dim
import load_product_bought_together_dimension as p_bought_together_dim
import load_product_buy_after_viewing_dimension as p_buy_after_view_dim
import load_product_review_fact as p_review_fact

import configparser

# Configuration Parameters

config = configparser.ConfigParser()
config.read('config.ini')
review_data_download_url=config['DownloadReviewData']['url']
dest_review_file_with_path=config['DownloadReviewData']['dest_file_with_path']

meta_data_download_url=config['DownloadProductMetaData']['url']
dest_meta_data_file_with_path=config['DownloadProductMetaData']['dest_file_with_path']


# Start of the workflow
class StartWorkFlow(luigi.Task):
    def requires(self):
        return None
    def output(self):
        return luigi.LocalTarget('./log/StartWorkflow.txt')
    def run(self):
        dt=datetime.datetime.now()
        with self.output().open('w') as outfile:
            outfile.write('Start Time : ')
            outfile.write(str(dt))

# Start of downloading the review data
class DownloadReviewData(luigi.Task):
    def requires(self):
        return StartWorkFlow()
    def output(self):
        return luigi.LocalTarget('./log/DownloadReviewData.txt')
    def run(self):
        sdd.file_download(review_data_download_url,dest_review_file_with_path)

# Start of downloading the review data
class DownloadProductMetaData(luigi.Task):
    def requires(self):
        return StartWorkFlow()
    def output(self):
        return luigi.LocalTarget('./log/DownloadProductMetaData.txt')
    def run(self):
        sdd.file_download(meta_data_download_url,dest_meta_data_file_with_path)

# Task - Product Extraction from JSON file into Parquet file
class ExtractProduct(luigi.Task):
    def requires(self):
        return DownloadProductMetaData()
    def output(self):
        return luigi.LocalTarget('./log/ExtractProduct.txt')
    def run(self):
        # Create the DB connection
        conn = db.connection()
        cur = conn.cursor()

        # Create the run information
        cur.execute('call amazon_review.proc_create_rundata(%s)', ('ExtractProduct',))
        conn.commit()

        # get the run id generated for the job
        cur.execute('SELECT * FROM amazon_review.func_return_runid(%s)', ('ExtractProduct',))
        run_id = cur.fetchone()

        prod.extract_product()
        with self.output().open('w') as outfile:
            outfile.write('Product extraction task ExtractProduct is done!\n')

        # Update the run status
        cur.execute('call amazon_review.proc_update_rundata(%s)', (run_id,))
        conn.commit()

        # closing db connection
        db.close(conn, cur)

# Task - Product Category Extraction from JSON file into Parquet file
class ExtractProdCategory(luigi.Task):
    def requires(self):
        return DownloadProductMetaData()
    def output(self):
        return luigi.LocalTarget('./log/ExtractProdCategory.txt')
    def run(self):
        # Create the DB connection
        conn = db.connection()
        cur = conn.cursor()

        # Create the run information
        cur.execute('call amazon_review.proc_create_rundata(%s)', ('ExtractProdCategory',))
        conn.commit()

        # get the run id generated for the job
        cur.execute('SELECT * FROM amazon_review.func_return_runid(%s)', ('ExtractProdCategory',))
        run_id = cur.fetchone()

        p_cat.extract_product_category()
        with self.output().open('w') as outfile:
            outfile.write('Product Category task ExtractProdCategory is done!\n')

        # Update the run status
        cur.execute('call amazon_review.proc_update_rundata(%s)', (run_id,))
        conn.commit()

        # closing db connection
        db.close(conn, cur)

# Task - Related Product Extraction from JSON file into Parquet file
class ExtractRelatedProduct(luigi.Task):
    def requires(self):
        return DownloadProductMetaData()
    def output(self):
        return luigi.LocalTarget('./log/ExtracRelatedProduct.txt')
    def run(self):
        # Create the DB connection
        conn = db.connection()
        cur = conn.cursor()

        # Create the run information
        cur.execute('call amazon_review.proc_create_rundata(%s)', ('ExtractRelatedProduct',))
        conn.commit()

        # get the run id generated for the job
        cur.execute('SELECT * FROM amazon_review.func_return_runid(%s)', ('ExtractRelatedProduct',))
        run_id = cur.fetchone()

        rel_prod.extract_related_product()
        with self.output().open('w') as outfile:
            outfile.write('Related product extraction task ExtractRelatedProduct is done!\n')

        # Update the run status
        cur.execute('call amazon_review.proc_update_rundata(%s)', (run_id,))
        conn.commit()

        # closing db connection
        db.close(conn, cur)

# Task - Product Category Dimension load from Parquet File
class LoadProductCategoryDimension(luigi.Task):
    def requires(self):
        return [ExtractProdCategory(),ExtractProduct()]
    def output(self):
        return luigi.LocalTarget('./log/LoadProductCategoryDimension.txt')
    def run(self):
        # Create the DB connection
        conn = db.connection()
        cur = conn.cursor()

        # Create the run information
        cur.execute('call amazon_review.proc_create_rundata(%s)', ('LoadProductCategoryDimension',))
        conn.commit()

        # get the run id generated for the job
        cur.execute('SELECT * FROM amazon_review.func_return_runid(%s)', ('LoadProductCategoryDimension',))
        run_id = cur.fetchone()

        p_cat_dim.load_product_category(run_id)
        with self.output().open('w') as outfile:
            outfile.write('The task LoadProductCategoryDimension is done')

        # Update the run status
        cur.execute('call amazon_review.proc_update_rundata(%s)', (run_id,))
        conn.commit()

        # closing db connection
        db.close(conn, cur)

# Task - Product Category Dimension load from Parquet File
class LoadProductDimension(luigi.Task):
    def requires(self):
        return [ExtractProduct(),LoadProductCategoryDimension()]
    def output(self):
        return luigi.LocalTarget('./log/LoadProductDimension.txt')
    def run(self):
        # Create the DB connection
        conn = db.connection()
        cur = conn.cursor()

        # Create the run information
        cur.execute('call amazon_review.proc_create_rundata(%s)', ('LoadProductDimension',))
        conn.commit()

        # get the run id generated for the job
        cur.execute('SELECT * FROM amazon_review.func_return_runid(%s)', ('LoadProductDimension',))
        run_id = cur.fetchone()

        p_dim.load_product_dimension(run_id)
        with self.output().open('w') as outfile:
            outfile.write('The task LoadProductDimension is done')

        # Update the run status
        cur.execute('call amazon_review.proc_update_rundata(%s)', (run_id,))
        conn.commit()

        # closing db connection
        db.close(conn, cur)

# Task - Also bought Product Dimension load from Parquet File
class LoadProdAlsoBoughtDimension(luigi.Task):
    def requires(self):
        return [ExtractRelatedProduct(),LoadProductDimension()]
    def output(self):
        return luigi.LocalTarget('./log/LoadProdAlsoBoughtDimension.txt')
    def run(self):
        # Create the DB connection
        conn = db.connection()
        cur = conn.cursor()

        # Create the run information
        cur.execute('call amazon_review.proc_create_rundata(%s)', ('LoadProdAlsoBoughtDimension',))
        conn.commit()

        # get the run id generated for the job
        cur.execute('SELECT * FROM amazon_review.func_return_runid(%s)', ('LoadProdAlsoBoughtDimension',))
        run_id = cur.fetchone()

        p_also_bought_dim.load_product_also_bought_dimension(run_id)
        with self.output().open('w') as outfile:
            outfile.write('The task LoadProdAlsoBoughtDimension is done')

        # Update the run status
        cur.execute('call amazon_review.proc_update_rundata(%s)', (run_id,))
        conn.commit()

        # closing db connection
        db.close(conn, cur)

# Task - Also viewed Product Dimension load from Parquet File
class LoadProdAlsoViewedDimension(luigi.Task):
    def requires(self):
        return [ExtractRelatedProduct(),LoadProductDimension()]
    def output(self):
        return luigi.LocalTarget('./log/LoadProdAlsoViewedDimension.txt')
    def run(self):
        # Create the DB connection
        conn = db.connection()
        cur = conn.cursor()

        # Create the run information
        cur.execute('call amazon_review.proc_create_rundata(%s)', ('LoadProdAlsoViewedDimension',))
        conn.commit()

        # get the run id generated for the job
        cur.execute('SELECT * FROM amazon_review.func_return_runid(%s)', ('LoadProdAlsoViewedDimension',))
        run_id = cur.fetchone()

        p_also_viewed_dim.load_product_also_viewed_dimension(run_id)
        with self.output().open('w') as outfile:
            outfile.write('The task LoadProdAlsoViewedDimension is done')

        # Update the run status
        cur.execute('call amazon_review.proc_update_rundata(%s)', (run_id,))
        conn.commit()

        # closing db connection
        db.close(conn, cur)

# Task - Bought together Product Dimension load from Parquet File
class LoadProdBoughtTogetherDimension(luigi.Task):
    def requires(self):
        return [ExtractRelatedProduct(),LoadProductDimension()]
    def output(self):
        return luigi.LocalTarget('./log/LoadProdBoughtTogetherDimension.txt')
    def run(self):
        # Create the DB connection
        conn = db.connection()
        cur = conn.cursor()

        # Create the run information
        cur.execute('call amazon_review.proc_create_rundata(%s)', ('LoadProdBoughtTogetherDimension',))
        conn.commit()

        # get the run id generated for the job
        cur.execute('SELECT * FROM amazon_review.func_return_runid(%s)', ('LoadProdBoughtTogetherDimension',))
        run_id = cur.fetchone()

        p_bought_together_dim.load_product_bought_together_dimension(run_id)
        with self.output().open('w') as outfile:
            outfile.write('The task LoadProdBoughtTogetherDimension is done')

        # Update the run status
        cur.execute('call amazon_review.proc_update_rundata(%s)', (run_id,))
        conn.commit()

        # closing db connection
        db.close(conn, cur)

# Task - Buy after viewing product Dimension load from Parquet File
class LoadProdBuyAfterViewingDimension(luigi.Task):
    def requires(self):
        return [ExtractRelatedProduct(),LoadProductDimension()]
    def output(self):
        return luigi.LocalTarget('./log/LoadProdBuyAfterViewingDimension.txt')
    def run(self):
        # Create the DB connection
        conn = db.connection()
        cur = conn.cursor()

        # Create the run information
        cur.execute('call amazon_review.proc_create_rundata(%s)', ('LoadProdBuyAfterViewingDimension',))
        conn.commit()

        # get the run id generated for the job
        cur.execute('SELECT * FROM amazon_review.func_return_runid(%s)', ('LoadProdBuyAfterViewingDimension',))
        run_id = cur.fetchone()

        p_buy_after_view_dim.load_product_buy_after_viewing(run_id)
        with self.output().open('w') as outfile:
            outfile.write('The task LoadProdBuyAfterViewingDimension is done')

        # Update the run status
        cur.execute('call amazon_review.proc_update_rundata(%s)', (run_id,))
        conn.commit()

        # closing db connection
        db.close(conn, cur)

# Task - Product Review data extraction from JSON file
class ExtractProductReview(luigi.Task):
    def requires(self):
        return DownloadReviewData()
    def output(self):
        return luigi.LocalTarget('./log/ExtractProductReview.txt')
    def run(self):
        # Create the DB connection
        conn = db.connection()
        cur = conn.cursor()

        # Create the run information
        cur.execute('call amazon_review.proc_create_rundata(%s)', ('ExtractProductReview',))
        conn.commit()

        # get the run id generated for the job
        cur.execute('SELECT * FROM amazon_review.func_return_runid(%s)', ('ExtractProductReview',))
        run_id = cur.fetchone()

        p_review.extract_product_review()
        with self.output().open('w') as outfile:
            outfile.write('The task ExtractProductReview is done')

        # Update the run status
        cur.execute('call amazon_review.proc_update_rundata(%s)', (run_id,))
        conn.commit()

        # closing db connection
        db.close(conn, cur)


# Task - Product Review data extraction from JSON file
class LoadProductReviewFact(luigi.Task):
    def requires(self):
        return [ExtractProductReview(),LoadProductDimension()]

    def output(self):
        return luigi.LocalTarget('./log/LoadProductReviewFact.txt')

    def run(self):
        # Create the DB connection
        conn = db.connection()
        cur = conn.cursor()

        # Create the run information
        cur.execute('call amazon_review.proc_create_rundata(%s)', ('LoadProductReviewFact',))
        conn.commit()

        # get the run id generated for the job
        cur.execute('SELECT * FROM amazon_review.func_return_runid(%s)', ('LoadProductReviewFact',))
        run_id=cur.fetchone()[0]

        print(run_id)

        # Call the program for data loading into DWH
        p_review_fact.load_product_review_fact(run_id)
        with self.output().open('w') as outfile:
            outfile.write('The task LoadProductReviewFact is done')

        # Update the run status
        cur.execute('call amazon_review.proc_update_rundata(%s)', (run_id,))
        conn.commit()

        # closing db connection
        db.close(conn, cur)

# Start of the workflow
class EndOfWorkflow(luigi.Task):
    def requires(self):
        return [
                    LoadProductDimension(),
                    LoadProductReviewFact(),
                    LoadProdAlsoBoughtDimension(),
                    LoadProdAlsoViewedDimension(),
                    LoadProdBoughtTogetherDimension(),
                    LoadProdBuyAfterViewingDimension()
                ]
    def output(self):
        return None
    def run(self):
        dt=datetime.datetime.now()
        for root, dirs, files in os.walk('./log'):
            for file in files:
                os.remove(os.path.join(root, file))

if __name__ == '__main__':
    luigi.run()