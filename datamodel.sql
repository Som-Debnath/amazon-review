/*
   -- datamodel.sql
   -- Contains the dimensional datamodel of amazon product review
   -- Created by: Som Debnath
   -- Creation Date: 01-March-2021
*/

DROP SCHEMA IF EXISTS amazon_review CASCADE;

create schema amazon_review;



create table IF NOT EXISTS amazon_review.d_product_category
   (
      prod_category_key serial  primary key,
	  category_name    varchar(100),
	  run_id       integer
   );


create table IF NOT EXISTS amazon_review.d_price_bucket
	(
		price_bucket_key serial primary key,
		price_bucket_desc varchar(50),
		bucket_lower_val  decimal,
		bucket_higher_val decimal,
		start_date   date,
	    end_date     date,
        active_flg   varchar(1)
	);


create table IF NOT EXISTS amazon_review.d_product
	(
	   product_key   	SERIAL 	PRIMARY KEY,
	   "asin"        		varchar(100),
	   product_title 		text,
	   product_desc         text,
	   brand         		text,
	   price         		decimal,
	   sales_rank    		integer,
	   start_date    		date,
	   end_date      		date,
       active_flg    		varchar(1),
	   sales_rank_cat_key 	integer,
	   prod_category_key  	integer,
	   price_bucket_key     integer,
	   hash_value           text,
	   run_id               integer
	);

create index idx_d_product_asin on amazon_review.d_product(asin);
create index idx_d_product_active_flg on amazon_review.d_product(active_flg);
create index idx_d_product_hash_val on amazon_review.d_product(hash_value);



create table IF NOT EXISTS amazon_review.d_date
	(
	   date_key integer,
	   "date" date,
	   "date_desc" varchar(20),
	   day_of_week varchar(10),
	   month_of_year varchar(10),
	   calendar_year integer
	);


create table IF NOT EXISTS amazon_review.f_product_review
(
   reviewer_id 			varchar(100),
   product_key 			integer,
   reviewer_name 		varchar(100),
   helpful 				text,
   review_text 			text,
   overall_rating 		decimal,
   review_summary 		text,
   unix_review_time   	integer,
   date_key             integer,
   run_id				integer
);


create table IF NOT EXISTS amazon_review.d_product_also_viewed
	(
		product_key          integer,
		also_viewed_asin varchar(100),
		run_id               integer
	);


create table IF NOT EXISTS amazon_review.d_product_also_bought
	(
		product_key integer,
		also_bought_asin varchar(100),
		run_id               integer
	);

create table IF NOT EXISTS amazon_review.d_product_bought_together
	(
		product_key integer,
		bought_together_asin varchar(100),
		run_id               integer
	);

create table IF NOT EXISTS amazon_review.d_product_buy_after_viewing
	(
		product_key integer,
		buy_after_viewing_asin varchar(100),
		run_id               integer
	);



create table IF NOT EXISTS amazon_review.t_job_information(
    job_id		integer primary key,
	job_name    varchar(100),
	job_desc    varchar(500),
	creation_dt  date
);


create table IF NOT EXISTS amazon_review.t_run_detail(
    run_id      SERIAL primary key,
	job_id      integer,     -- Foreign Key
	run_date    date,
	run_start_time    varchar(10),
	run_end_time      varchar(10),
	run_status     varchar(50),
	CONSTRAINT fk_job_info
      FOREIGN KEY(job_id)
	  REFERENCES amazon_review.t_job_information(job_id)
);


/*
     Insert Script t_job_information
*/

INSERT INTO amazon_review.t_job_information (job_id,job_name,job_desc,creation_dt)
                      values (1,'ExtractProduct','Reading product data from source file',current_date);

INSERT INTO amazon_review.t_job_information (job_id,job_name,job_desc,creation_dt)
                      values (2,'ExtractProdCategory','Reading product category data from source file',current_date);

INSERT INTO amazon_review.t_job_information (job_id,job_name,job_desc,creation_dt)
                      values (3,'ExtractRelatedProduct','Reading related product data from source file',current_date);

INSERT INTO amazon_review.t_job_information (job_id,job_name,job_desc,creation_dt)
                      values (4,'ExtractProductReview','Reading product review data from source file',current_date);

INSERT INTO amazon_review.t_job_information (job_id,job_name,job_desc,creation_dt)
                      values (5,'LoadProductCategoryDimension','Loading product category dimension in the DWH',current_date);

INSERT INTO amazon_review.t_job_information (job_id,job_name,job_desc,creation_dt)
                      values (6,'LoadProductDimension','Loading product dimension in the DWH',current_date);

INSERT INTO amazon_review.t_job_information (job_id,job_name,job_desc,creation_dt)
                      values (7,'LoadProdAlsoBoughtDimension','Loading product also bought dimension in the DWH',current_date);

INSERT INTO amazon_review.t_job_information (job_id,job_name,job_desc,creation_dt)
                      values (8,'LoadProdAlsoViewedDimension','Loading product also viewed dimension in the DWH',current_date);

INSERT INTO amazon_review.t_job_information (job_id,job_name,job_desc,creation_dt)
                      values (9,'LoadProdBoughtTogetherDimension','Loading product bought together in the DWH',current_date);

INSERT INTO amazon_review.t_job_information (job_id,job_name,job_desc,creation_dt)
                      values (10,'LoadProdBuyAfterViewingDimension','Loading product bu after viewing in the DWH',current_date);

INSERT INTO amazon_review.t_job_information (job_id,job_name,job_desc,creation_dt)
                      values (11,'LoadProductReviewFact','Loading product review fact in the DWH',current_date);

commit;

/*

    Insert Script d_price_bucket

*/

INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('Outlier in lower limit',-9999999.0,-1,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('0-25.0 price band',0.0,25.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('25.01-50.0 price band',25.01,50.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('50.01-75.0 price band',50.01,75.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('75.01-100.0 price band',75.01,100.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('100.01-125.0 price band',100.01,125.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('125.01-150.0 price band',125.01,150.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('150.01-175.0 price band',150.01,175.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('175.01-200.0 price band',175.01,200.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('200.01-225.0 price band',200.01,225.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('225.01-250.0 price band',225.01,250.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('250.01-275.0 price band',250.01,275.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('275.01-300.0 price band',275.01,300.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('300.01-325.0 price band',300.01,325.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('325.01-350.0 price band',325.01,350.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('350.01-375.0 price band',350.01,375.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('375.01-400.0 price band',375.01,400.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('400.01-425.0 price band',400.01,425.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('425.01-450.0 price band',425.01,450.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('450.01-475.0 price band',450.01,475.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('475.01-500.0 price band',475.01,500.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('500.01-525.0 price band',500.01,525.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('525.01-550.0 price band',525.01,550.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('550.01-575.0 price band',550.01,575.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('575.01-600.0 price band',575.01,600.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('600.01-625.0 price band',600.01,625.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('625.01-650.0 price band',625.01,650.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('650.01-675.0 price band',650.01,675.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('675.01-700.0 price band',675.01,700.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('700.01-725.0 price band',700.01,725.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('725.01-750.0 price band',725.01,750.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('750.01-775.0 price band',750.01,775.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('775.01-800.0 price band',775.01,800.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('800.01-825.0 price band',800.01,825.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('825.01-850.0 price band',825.01,850.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('850.01-875.0 price band',850.01,875.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('875.01-900.0 price band',875.01,900.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('900.01-925.0 price band',900.01,925.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('925.01-950.0 price band',925.01,950.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('950.01-975.0 price band',950.01,975.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('975.01-1000.0 price band',975.01,1000.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');
INSERT INTO amazon_review.d_price_bucket (price_bucket_desc,bucket_lower_val,bucket_higher_val,start_date,end_date,active_flg)
               VALUES('1000.01-9999999.0 outlier in upperlimit',1000.01,9999999.0,current_date,to_date('31-12-9999','DD-MM-YYYY'),'Y');


/*
     Default values in dimension
*/

insert into amazon_review.d_product_category(prod_category_key,category_name,run_id) values(-1,'UNKNOWN',0);

INSERT INTO amazon_review.d_product(
       product_key   	 ,
	   "asin"        	,
	   product_title 	,
	   product_desc      ,
	   brand         	,
	   price         	,
	   sales_rank    	,
	   start_date    	,
	   end_date      	,
       active_flg    	,
	   sales_rank_cat_key,
	   prod_category_key ,
	   price_bucket_key  ,
	   hash_value        ,
	   run_id  ) VALUES(-1,'N/A','N/A','N/A','N/A',null,null,current_date,to_date('31129999','DDMMYYYY'),'Y',-1,-1,0,0,0);

commit;

/*
   Date dimension
*/


create or replace procedure amazon_review.proc_load_date_dimension(
   p_start_date date,
   p_end_date date
)
language plpgsql
as $$
declare
       l_diff integer := 0;
	   l_date date;
	   l_count integer :=0;
	   l_rec_inserted integer := 0;
begin
      IF p_start_date < p_end_date THEN

	        l_diff := p_end_date - p_start_date;

			for i in  0..l_diff  loop

				l_date := p_start_date+i;

				select count(*) into l_count from amazon_review.d_date where date_key=to_char(l_date,'YYYYMMDD')::int;

				if l_count<1 then

				insert into amazon_review.d_date
							(
							   date_key ,
							   "date" ,
							   date_desc ,
							   day_of_week,
							   month_of_year,
							   calendar_year
							) values(
							    to_char(l_date,'YYYYMMDD')::int,
								l_date,
								to_char(l_date,'Month')||' '||
                                     (to_char(l_date,'dd')::int)::varchar||', '||
									 to_char(l_date,'YYYY'),
							    to_char(l_date,'Day'),
								to_char(l_date,'Month'),
								to_char(l_date,'YYYY')::int

							);
				commit;
			    l_rec_inserted := l_rec_inserted +1;

                end if;

			end loop;

	  END IF;

	raise notice 'Number of records inserted %',l_rec_inserted;

end;
$$
;

/*

   Stored programs proc_load_product_also_viewed

*/


create or replace procedure amazon_review.proc_load_product_also_viewed(
   p_runid int
)
language plpgsql
as $$
declare
       l_count    integer := 0;
begin

    /*
	    Creating the index on temp tables
	*/
    select
         count(distinct i.relname) INTO l_count
	from
		pg_class t,
		pg_class i,
		pg_index ix,
		pg_attribute a
	where
		t.oid = ix.indrelid
		and i.oid = ix.indexrelid
		and a.attrelid = t.oid
		and a.attnum = ANY(ix.indkey)
		and t.relkind = 'r'
		and t.relname like 'temp_product_also_viewed'
	;

	IF l_count<1 THEN
	    create index idx_temp_product_buy_also_viewed on amazon_review.temp_product_also_viewed(asin,also_viewed);
	END IF;

	insert into amazon_review.d_product_also_viewed (product_key,also_viewed_asin,run_id)
	select
	      new_data.product_key,
	      new_data.also_viewed_asin,
	      p_runid as run_id
	from
	(select distinct
	        B.product_key,
	        A.also_viewed as also_viewed_asin
	       from
	            amazon_review.temp_product_also_viewed A,
	            amazon_review.d_product B
	       where
	            A.asin = B.asin) new_data left join
	       amazon_review.d_product_also_viewed existing_date
	       on (new_data.product_key=existing_date.product_key
	          and new_data.also_viewed_asin=existing_date.also_viewed_asin)
	   where
	         existing_date.product_key is null;

end;
$$
;

/*
   Stored programs proc_load_product_also_bought
*/

create or replace procedure amazon_review.proc_load_product_also_bought(
   p_runid int
)
language plpgsql
as $$
declare
       l_count    integer := 0;
begin

    /*
	    Creating the index on temp tables
	*/
    select
         count(distinct i.relname) INTO l_count
	from
		pg_class t,
		pg_class i,
		pg_index ix,
		pg_attribute a
	where
		t.oid = ix.indrelid
		and i.oid = ix.indexrelid
		and a.attrelid = t.oid
		and a.attnum = ANY(ix.indkey)
		and t.relkind = 'r'
		and t.relname like 'temp_product_also_bought'
	;

	IF l_count<1 THEN
	    create index idx_temp_product_also_bought on amazon_review.temp_product_also_bought(asin,also_bought);
	END IF;

	insert into amazon_review.d_product_also_bought (product_key,also_bought_asin,run_id)
	select
	      new_data.product_key,
	      new_data.also_bought_asin,
	      p_runid as run_id
	from
	(select distinct
	        B.product_key,
	        A.also_bought as also_bought_asin
	       from
	            amazon_review.temp_product_also_bought A,
	            amazon_review.d_product B
	       where
	            A.asin = B.asin) new_data left join
	       amazon_review.d_product_also_bought existing_date
	       on (new_data.product_key=existing_date.product_key
	          and new_data.also_bought_asin=existing_date.also_bought_asin)
	   where
	         existing_date.product_key is null;
end;
$$
;

/*
   Stored programs proc_load_product_bought_together
*/

create or replace procedure amazon_review.proc_load_product_bought_together(
   p_runid int
)
language plpgsql
as $$
declare
       l_count    integer := 0;
begin

    /*
	    Creating the index on temp tables
	*/
    select
         count(distinct i.relname) INTO l_count
	from
		pg_class t,
		pg_class i,
		pg_index ix,
		pg_attribute a
	where
		t.oid = ix.indrelid
		and i.oid = ix.indexrelid
		and a.attrelid = t.oid
		and a.attnum = ANY(ix.indkey)
		and t.relkind = 'r'
		and t.relname like 'temp_product_bought_together'
	;

	IF l_count<1 THEN
	    create index idx_temp_product_bought_together on amazon_review.temp_product_bought_together(asin,bought_together);
	END IF;

	insert into amazon_review.d_product_bought_together (product_key,bought_together_asin,run_id)
	select
	      new_data.product_key,
	      new_data.bought_together_asin,
	      p_runid as run_id
	from
	(select distinct
	        B.product_key,
	        A.bought_together as bought_together_asin
	       from
	            amazon_review.temp_product_bought_together A,
	            amazon_review.d_product B
	       where
	            A.asin = B.asin) new_data left join
	       amazon_review.d_product_bought_together existing_date
	       on (new_data.product_key=existing_date.product_key
	          and new_data.bought_together_asin=existing_date.bought_together_asin)
	   where
	         existing_date.product_key is null;

end;
$$
;


/*
    Stored programs proc_load_product_buy_after_viewing
*/

create or replace procedure amazon_review.proc_load_product_buy_after_viewing(
   p_runid int
)
language plpgsql
as $$
declare
       l_count    integer := 0;
begin

    /*
	    Creating the index on temp tables
	*/
    select
         count(distinct i.relname) INTO l_count
	from
		pg_class t,
		pg_class i,
		pg_index ix,
		pg_attribute a
	where
		t.oid = ix.indrelid
		and i.oid = ix.indexrelid
		and a.attrelid = t.oid
		and a.attnum = ANY(ix.indkey)
		and t.relkind = 'r'
		and t.relname like 'temp_product_buy_after_viewing'
	;

	IF l_count<1 THEN
	    create index idx_temp_product_buy_after_viewing on amazon_review.temp_product_buy_after_viewing(asin,buy_after_viewing);
	END IF;

	insert into amazon_review.d_product_buy_after_viewing (product_key,buy_after_viewing_asin,run_id)
	select
	      new_data.product_key,
	      new_data.buy_after_viewing_asin,
	      p_runid as run_id
	from
	(select distinct
	        B.product_key,
	        A.buy_after_viewing as buy_after_viewing_asin
	       from
	            amazon_review.temp_product_buy_after_viewing A,
	            amazon_review.d_product B
	       where
	            A.asin = B.asin) new_data left join
	       amazon_review.d_product_buy_after_viewing existing_date
	       on (new_data.product_key=existing_date.product_key
	          and new_data.buy_after_viewing_asin=existing_date.buy_after_viewing_asin)
	   where
	         existing_date.product_key is null;

end;
$$
;

/*
     Product Category dimension loading procedure
*/

CREATE OR REPLACE PROCEDURE amazon_review.proc_load_product_category_dimension(p_runid integer)
 LANGUAGE plpgsql
AS $procedure$
declare
       l_count integer:=0;
       l_record   record;
begin

		   INSERT INTO amazon_review.d_product_category
			(
				category_name,
				run_id
				)
			SELECT
			     A.category_name,
				 p_runid as run_id
			FROM
			      amazon_review.temp_product_category A left join
				  amazon_review.d_product_category B
			   ON (A.category_name=B.category_name)
			WHERE
			     B.prod_category_key IS NULL
				 ;


	raise notice 'Successfully Done';

end;
$procedure$
;


/*
    Product dimension loading procedure
*/

CREATE OR REPLACE PROCEDURE amazon_review.proc_load_product_dimension(p_runid integer)
 LANGUAGE plpgsql
AS $procedure$
declare
       l_count integer:=0;
       l_record   record;
begin
     /*
	     Update path
	 */

	  create local temporary table temp_$$_amazon_prod_update
	  as
	  SELECT
			      t.asin,
				  t.product_title,
				  t.product_desc,
				  t.brand,
				  t.price,
				  t.sales_rank,
				  t.sales_rank_cat_key,
				  t.prod_category_key,
				  t.price_bucket_key,
				  t.hash_value,
				  p.product_key
			FROM  amazon_review.d_product p inner join
			      (select
				        t.asin,
						  t.product_title,
						  t.product_desc,
						  t.brand,
						  t.price,
						  t.sales_rank,
						  t.sales_rank_cat_key,
						  t.prod_category_key,
						  t.price_bucket_key,
						md5(
						   t.sales_rank_cat_key ::varchar||
						   t.prod_category_key::varchar||
						   coalesce(t.product_title,'NULL')||
						   coalesce(t.product_desc,'NULL')||
						   coalesce(t.brand,'NULL')||
						   t.price::varchar||
						   coalesce(t.sales_rank,-1)::varchar||
						   t.price_bucket_key::varchar
						   ) as hash_value
						   from
						       amazon_review.temp_product t) t
			  ON  p.asin=t.asin
		   WHERE  p.active_flg='Y'
		     and  p.hash_value is not null
			 and  p.hash_value<>t.hash_value;

	    FOR l_record in (select * from temp_$$_amazon_prod_update)
	    LOOP
		   update amazon_review.d_product set
				   end_date=current_date,
				   active_flg='N',
				   run_id=p_runid
		         where product_key=l_record.product_key;

		    INSERT INTO amazon_review.d_product
			(
				asin,
				product_title,
				product_desc,
				brand,
				price,
				sales_rank,
				start_date,
				end_date,
				active_flg,
				sales_rank_cat_key,
				prod_category_key,
				price_bucket_key,
                hash_value,
				run_id
				)
			VALUES(
			    l_record.asin,
				l_record.product_title,
				l_record.product_desc,
				l_record.brand,
				l_record.price,
				l_record.sales_rank,
				current_date,
				to_date('31-12-9999','DD-MM-YYYY'),
				'Y',
				l_record.sales_rank_cat_key,
				l_record.prod_category_key,
				l_record.price_bucket_key,
                l_record.hash_value,
				p_runid);

	  end loop;


	  DROP TABLE temp_$$_amazon_prod_update;

	  /*
	     Insert path
	 */

		   INSERT INTO amazon_review.d_product
			(
				asin,
				product_title,
				product_desc,
				brand,
				price,
				sales_rank,
				start_date,
				end_date,
				active_flg,
				sales_rank_cat_key,
				prod_category_key,
				price_bucket_key,
				hash_value,
				run_id
				)
			SELECT
			          t.asin,
					  t.product_title,
					  t.product_desc,
					  t.brand,
					  t.price,
					  t.sales_rank,
					  current_date as start_date ,
				      to_date('31-12-9999','DD-MM-YYYY') as end_date,
				      'Y' as active_flg ,
					  t.sales_rank_cat_key,
					  t.prod_category_key,
					  t.price_bucket_key,
					  md5(
						   t.sales_rank_cat_key ::varchar||
						   t.prod_category_key::varchar||
						   coalesce(t.product_title,'NULL')||
						   coalesce(t.product_desc,'NULL')||
						   coalesce(t.brand,'NULL')||
						   t.price::varchar||
						   coalesce(t.sales_rank,-1)::varchar||
						   t.price_bucket_key::varchar
						   ) as hash_value,
					       p_runid as run_id
		           FROM  amazon_review.temp_product t left join amazon_review.d_product p
				   ON  p.asin=t.asin
				   where
				        p.product_key is null;


	raise notice 'Successfully Done';

end;
$procedure$
;


/*
    Run Info update stored programs

*/


CREATE OR REPLACE PROCEDURE amazon_review.proc_create_rundata(
     p_job_name varchar
)
 LANGUAGE plpgsql
AS $procedure$
declare
        l_jobid integer := -1;
begin
	   /*
	    *  Selecting the job_id from job_name
	    *
	    */
	   select job_id into l_jobid from amazon_review.t_job_information where job_name = p_job_name;

	   /*
	    *  Insert the new run data into run details
	    */
	  INSERT INTO amazon_review.t_run_detail (job_id, run_date, run_start_time, run_end_time, run_status)
	       VALUES(l_jobid,current_date, to_char(CURRENT_TIMESTAMP,'HH24:MI:SS'), '', 'Running');

end;
$procedure$
;


CREATE OR REPLACE PROCEDURE amazon_review.proc_update_rundata(
     p_runid integer
)
 LANGUAGE plpgsql
AS $procedure$
begin

	   /*
	    *  update the run status of the job
	    */

	  update amazon_review.t_run_detail
	      set
	          run_end_time = to_char(CURRENT_TIMESTAMP,'HH24:MI:SS'),
	          run_status = 'Completed'
	  where run_id = p_runid;

end;
$procedure$
;



create or replace function amazon_review.func_return_runid(
     p_job_name varchar
)
returns integer
language plpgsql
as
$$
declare

       l_jobid integer := -1;
       l_runid integer := -1;

begin

	   /*
	    *  Selecting the job_id from job_name
	    *
	    */

	    select job_id into l_jobid from amazon_review.t_job_information where job_name = p_job_name;

		/*
		 *  Getting the latest runid for a job run
		 */

	   select max(run_id) into l_runid from amazon_review.t_run_detail where job_id = l_jobid;


	  return l_runid;

end;
$$
;