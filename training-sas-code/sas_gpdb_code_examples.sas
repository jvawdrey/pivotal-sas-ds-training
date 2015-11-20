/*
**
** Pivotal-SAS Data Science Training
** Code Examples
**
*/


/* *********** Setup ********** */

* System options;
options threads=yes sastrace=',,t,dsab'  sastraceloc=saslog NOSTSUFFIX;

* Libname statement;
libname mydblib greenplm server=&server. database=&database. port=&port. user=&user. password=&password. schema=&schema. dbcommit=100;


/* *********** Data step & PROC SQL ********** */

* Data step example: Extract observations where make is equal to BMW’ – libref mydblib points to database schema ‘sasgpdb’;
data local_cars;
  set mydblib.cars (
    where=(make='BMW')
  );
run;

* Data step example: Create database table test1 and distribute randomly;
data mydblib.test1
  (distributed_by='DISTRIBUTED RANDOMLY');
  id = 1;
run;

* Data step example: Create database table test2 and distribute by column "id";
data mydblib.test2
  (distributed_by='distributed by (id)');
  id = 1;
run;

* Data step example: Create database table test2 and distribute by column "id";
* Override default types;
data mydblib.test3(
  dbtype=(
    id  = 'integer'
    item  = 'varchar(20)'
    amount  = 'numeric(18,5)'
    trxn_date  = 'date')
  distributed_by='distributed by (id)'
  );
  id = 1;
  item = 'DS Services';
  amount = 16000;
  trxn_date = '01jan2015'd;
  format trxn_date date9.;
run;

* Data step example: Create database table test2 and distribute by column "id";
* Override default types;
* Add insertbuff of 5;
data mydblib.test4(
  dbtype=(
    name  = 'varchar(8)'
    sex  = 'varchar(1)'
    age  = 'integer'
    height  = 'numeric(5,2)'
    weight  = 'numeric(5,2)')
  distributed_by='distributed by (name)'
  insertbuff=5
  );
  set sashelp.class;
run;

* Proc SQL example: Extract observations where make is equal to BMW – libref mydblib points to database schema ‘sasgpdb’;
proc sql;
  CREATE TABLE local_cars2 AS
  SELECT *
  FROM mydblib.cars
  WHERE make='BMW';
quit;


/* *********** SQL Pass-through facility ********** */

* View MADlib version installed in database (this query will fail);
proc sql;
  SELECT madlib.version();
quit;

* Try again using SQL Pass-through facility;
proc sql;
  * Open connection to the database;
  CONNECT TO greenplm AS gpcon (server=&server. database=&database. port=&port. user=&user. password=&password.);

  * Call MADlib version function and print results to SAS Results Viewer;
  SELECT *
  FROM connection to gpcon (
    SELECT madlib.version()
  );

  * Close open connections;
  DISCONNECT FROM gpcon;
quit;

* Second SQL Pass-through facility example - this one uses EXECUTE;
proc sql;
  * SQL Procedure Pass-Through Facility;
  CONNECT TO greenplm AS gpcon (server=&server. database=&database. port=&port. user=&user. password=&password.);

  * Note - GPDB does not automatically overwrite tables - we first need to drop in cases where table already exist;
  EXECUTE (
    DROP TABLE IF EXISTS &schema..madlibtest
  ) BY gpcon;

  * Create database table with one record and one column containing output of MADlib version() function;
  * Note - Query executed in-database and therefore restricted from creating table in client library;
  EXECUTE (
    CREATE TABLE &schema..madlibtest AS
	SELECT madlib.version()
	DISTRIBUTED RANDOMLY
  ) BY gpcon;

  * Close open connections;
  DISCONNECT FROM gpcon;
quit;

* View table;
proc print data=mydblib.madlibtest;
run;

* Create a database table 'this_is_a_test' in order to test catalog queries;
proc sql;
  * SQL Procedure Pass-Through Facility;
  CONNECT TO greenplm AS gpcon (server=&server. database=&database. port=&port. user=&user. password=&password.);

  EXECUTE (
    CREATE TABLE &schema..this_is_a_test (i int);
  ) BY gpcon;

  * Close open connections;
  DISCONNECT FROM gpcon;
quit;

* Catalog Queries - Example #1;
proc sql;
  * SQL Procedure Pass-Through Facility;
  CONNECT TO greenplm AS gpcon (server=&server. database=&database. port=&port. user=&user. password=&password.);

  * SQLTables;
  SELECT *
  FROM connection to gpcon (
    Greenplum::SQLTables "","&schema.",'this_is_a_test'
  );

  * Close open connections;
  DISCONNECT FROM gpcon;
quit;

* Catalog Queries - Example #2;
proc sql;
  * SQL Procedure Pass-Through Facility;
  CONNECT TO greenplm AS gpcon (server=&server. database=&database. port=&port. user=&user. password=&password.);

  * SQLColumns;
  SELECT *
  FROM connection to gpcon (
    Greenplum::SQLColumns "","&schema.",'this_is_a_test'
  );

  * Close open connections;
  DISCONNECT FROM gpcon;
quit;

/* *********** SAS Formats & User Defined Formats ********** */

* Copy sashelp.stocks to database;
data mydblib.stocks;
  set sashelp.stocks;
run;

* Find the number of records with date equal to January 1st;
proc sql;
  SELECT count(*)
  FROM mydblib.stocks
  WHERE (put(date, date5.) = '01JAN');
quit;

* Create user defined format – requires all inclusive format;
proc format;
  value udfmt 0-20000='low' 20001-40000='medium' 40001-60000='high';
run;

* Find the number of autos with price between $0 and $20,000;
proc sql;
  SELECT count(*)
  FROM mydblib.cars
  WHERE put(msrp, udfmt.) = 'low';
quit;


/* *********** Threaded Reads & Auto Partitioning ********** */

* Set sastrace to view threads;
options threads=yes sastrace=',,t,dsab' sastraceloc=saslog NOSTSUFFIX;

* Find cutoffs (~ equal depth);
* Note - this could be automated using a MACRO to find optimal breaks;
proc sql;
  SELECT CASE WHEN msrp <= 23000 THEN 1
              WHEN msrp <= 35000 THEN 2
			  ELSE 3 END AS class
        ,count(*) AS cnt
  FROM mydblib.cars
  GROUP BY class
  ORDER BY class;
quit;

* Copy db table cars to local SAS library;
data local;
  set mydblib.cars(
    DBSLICE=(
      "msrp <= 23000"
      "23000<msrp and msrp<=35000"
      "35000 < msrp"
    )
  );
run;
* Note – 4 missing values exist;

/* *********** User Defined Database Functions ********** */

* Prepare string with UDF;
%let udf = %str(
  CREATE FUNCTION &schema..udftest(
    str text
  ) RETURNS text AS
  $BODY$
  DECLARE
    val text;
  BEGIN
    val := 'Hello '||str||'!';
    RETURN val;
  END;
  $BODY$
  LANGUAGE 'plpgsql';
);

* Print string to console;
%put &udf;

* Add function to database;
proc sql;
  * SQL Procedure Pass-Through Facility;
  CONNECT TO greenplm AS gpcon 	(server=&server. db=&database. 	port=&port. user=&user. password=&password.);

  * Add UDF to database;
  EXECUTE (
    &udf.
  ) BY gpcon;

  * Close open connections;
  DISCONNECT FROM gpcon;
quit;

* Test function;
proc sql;
  * SQL Procedure Pass-Through Facility;
  CONNECT TO greenplm AS gpcon (server=&server. db=&database. port=&port. user=&user. password=&password.);

  * Note - GPDB does not automatically overwrite tables - we first need to drop in cases where table already exist;
  EXECUTE (
    DROP TABLE IF EXISTS &schema..udftest
  ) BY gpcon;

  * Test UDF by creating a new table with output of function call;
  EXECUTE (
    CREATE TABLE &schema..udftest AS
	SELECT &schema..udftest('SAS User')
	DISTRIBUTED RANDOMLY;
  ) BY gpcon;

  * Another way to view results (in SAS Results Viewer);
  SELECT *
  FROM connection to gpcon (
    SELECT &schema..udftest('SAS User')
  );

  * Close open connections;
  DISCONNECT FROM gpcon;
quit;

* View database table;
proc print data=mydblib.udftest;
run;

/* *********** MADlib In-database Scoring ********** */

* Establish connection to database;
libname mydblib greenplm server=&server. port=&port. database=&database. user=&user. password=&password. schema=&schema.;

* Linear regression model predicting mpg_highway;
proc reg data=mydblib.cars;
  model mpg_highway=Horsepower Weight EngineSize;
  * Output parameter estimates to a dataset;
  ods output parameterEstimates = sas_mpg_highway_lrm;
run;
quit;

* Run model publishing macro;
%madlib_sas_publish_lr(
  modelDataset=sas_mpg_highway_lrm
 ,modelTable=&schema..db_mpg_highway_lrm
 ,server=&server
 ,db=&database
 ,port=&port
 ,user=&user
 ,password=&password
 ,drop=1
);

* MADlib scoring MACRO;
%madlib_sas_score_lr(
  inTable=&schema..cars
 ,outTable=&schema..cars_scored
 ,modelTable=&schema..db_mpg_highway_lrm
 ,server=&server
 ,db=&database
 ,port=&port
 ,user=&user
 ,password=&password
 ,predictColumnName=m_prediction
 ,residualColumnName=m_error
 ,drop=1
);


/* *********** Clean Up ********** */

* Clear libname;
libname mydblib clear;
