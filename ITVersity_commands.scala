/******itversity sqoop*******/
MAX_ORDER_ID=`sqoop eval --connect "jdbc:mysql://nn01.itversity.com:3306/retail_db" username retail_dba --password itversity --query "select max(order_id) from orders" 2>/dev/null|grep "^|"|grep -v max|
awk -F" " '{print $2}' `;

/*****incremental sqoop*****/
sqoop import --connect "jdbc:mysql://nn01.itversity.com:3306/retail_db" \
--username retail_dba \
--password itversity \
--table orders \
--target-dir hdfs://nn01.itversity.com:8020/apps/hive/warehouse/phiripatrick663.db/orders \
--append \
--fields-terminated-by '|' \
--where "order_id" <= 60000


sqoop import --connect "jdbc:mysql://nn01.itversity.com:3306/retail_db" \
--username retail_dba \
--password itversity \
--table orders \
--target-dir hdfs://nn01.itversity.com:8020/apps/hive/warehouse/phiripatrick663.db/orders \
--append \
--fields-terminated-by '|' \
--check-column order_id \
--incremental append \
--last-value 60000

/******use staging table*****/
create table dg_orders_staging as select * from dg_orders where 1=2;

$sqoop export --connect "jdbc:mysql://nn01.itversity.com:3306/retail_db" \
--username retail_dba \
--password itversity \
--export-dir /user/phiripatrick663/retail_db/orders \
--table orders \
--input-fields-terminated-by '|' \
----input-null-non-string  '\\N'  \
--input-null-string  '\\N'  \
--columns order_id, order_date, order_customer_id, orders_status \
--staging-table dg_orders_staging

/****Merging data****/

hadoop fs -mkdir /user/cloudera/sqoop_merge

/****Initial load****/
sqoop import --connect "jdbc:mysql://quicksart.cloudera:3306/retail_db" \
--username retail_dba \
--password cloudera \
--table departmenst \
--as-textfile \
--target-dir /user/cloudera/sqoop_merge/departments

/*****validate*****/
sqoop eval --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username retail_dba \
--password cloudera \
--query "select * from departments"


/*****update*****/
sqoop eval --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username retail_dba \
--password cloudera \
--query "update departments set department_name=Testing Merge' where department_id = 9000"

/*****insert*****/
sqoop eval --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username retail_dba \
--password cloudera \
--query "insert into departments values (10000, 'Inserting for merge')"

/*****validate*****/
sqoop eval --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username retail_dba \
--password cloudera \
--query "select * from departments"

/*****new load*****/
sqoop import --connect "jdbc:mysql://quicksart.cloudera:3306/retail_db" \
--username retail_dba \
--password cloudera \
--table departmenst \
--as-textfile \
--target-dir /user/cloudera/sqoop_merge/departments_delta \
--where "department_id >= 9000"

hadoop fs - cat /user/cloudera/sqoop_merge/departments_delta/part*

/*****merge*****/
sqoop merge --merge-key department_id \
--new-data /user/cloudera/sqoop_merge/departments_delta \
--onto /user/cloudera/sqoop_merge/departments \
--target-dir /user/cloudera/sqoop_merge/departments_stage \
--class-name departments \
--jar-file

hadoop fs - cat /user/cliudera/sqoop_merge/departments_stage/part*

/*****delete old directory******/
hadoop fs -rm -R /user/cloudera/sqoop_merge/departments

/*****move/rename stage directory to original directory*****/
hadoop fs -mv /user/cloudera/sqoop_merge/departments_stage /user/cloudera/sqoop_merge/departments

/*****validate that original directory have merged data*****/
hadoop fs -cat /user/cloudera/sqop_merge/departments/part*

/*****CREATE TABLES*****/
CREATE TABLE stocks_eod_managed
(
	stockticker string;
	tradedate int;
	openprice float;
	highprice float;
	lowprice float;
	closeprice float;
	volume bigint
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/phiripatrick663/nyse_hive';   //location mandetory for external table


CREATE TABLE stocks_eod_managed
(
	stockticker string;
	tradedate int;
	openprice float;
	highprice float;
	lowprice float;
	closeprice float;
	volume bigint
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/data/nyse/' INTO TABLE stocks_eod_managed;

/*****validate*****/
wc -l /data/nyse/*.txt   */ OR
select count(1) from stocks_eod_managed



/*******PARTITIONING*****/
//HIVE HAS ONLY LIST and HASH PARTITIONING
//PARTITIONED BY is LIST partitioning
//CLUSTERED BY is HASH partitioning
CREATE TABLE stocks_eod_list
(
	stockticker string;
	tradedate int;
	openprice float;
	highprice float;
	lowprice float;
	closeprice float;
	volume bigint
)
PARTITION BY LIST(tradedate int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';

ALTER TABLE stocks_eod_list ADD PARTITION (tradeyear=2001);
ALTER TABLE stocks_eod_list ADD PARTITION (tradeyear=2002);
ALTER TABLE stocks_eod_list ADD PARTITION (tradeyear=2003);
ALTER TABLE stocks_eod_list ADD PARTITION (tradeyear=2004);
ALTER TABLE stocks_eod_list ADD PARTITION (tradeyear=2005);

LOAD DATA LOCAL INPATH '/data/nyse/' INTO TABLE stocks_eod_list PARTITION(tradeyear=2001);         //loading from local file system,preformatted data
LOAD DATA LOCAL INPATH '/data/nyse/' INTO TABLE stocks_eod_list PARTITION(tradeyear=2002);
LOAD DATA LOCAL INPATH '/data/nyse/' INTO TABLE stocks_eod_list PARTITION(tradeyear=2003);
LOAD DATA LOCAL INPATH '/data/nyse/' INTO TABLE stocks_eod_list PARTITION(tradeyear=2004);
LOAD DATA LOCAL INPATH '/data/nyse/' INTO TABLE stocks_eod_list PARTITION(tradeyear=2005);

INSERT INTO TABLE stocks_eod_list PARTITION (tradeyear)                                                            //USE if there is transformation of data
SELECT t.* cast(substr(tradedate, 1,4) as int) tradeyear
FROM stocks_eod_managed t;

CREATE TABLE orders_another(
	`order_id` string,
	`order_date` string,
	`order_customer_id` int,
	`orders_status` varchar(45)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

LOAD DATA LOCAL INPATH '/data/nyse'  INTO TABLE orders_another;   //If file format is mismatched NULL values will be loaded into table

/**validate**/
describe formatted orders_another;                                                                                                       //get file location
dfs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/phiripatrick663.db/orders_another;
hdfs fsck hdfs://nn01.itversity.com:8020/apps/hive/warehouse/phiripatrick663.db/orders_another -files -location -blocks  //get metadata

/***CLUSTERED BY is HASH partitioning***/
//gives better uniform distribution
CREATE TABLE `orders_bucketed` (
	`order_id` string,
	`order_date` string,
	`order_customer_id` int,
	`orders_status` varchar(45)
)
CLUSTERED BY (order_id) INTO 16 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;
