/*
**
** Pivotal-SAS Data Science Training
** Setup
**
*/

/* *********** Connecting to Greenplum/HAWQ (Libname) ********** */

* Connection details;
%let server=; * IP address example %let server='192.0.0.1';
%let port=;
%let database=;
%let user=;
%let password=;
%let schema=;

* Libname statement;
libname mydblib greenplm server=&server. database=&database. port=&port. user=&user. password=&password. schema=&schema.;

/*
NOTE: Libref MYDBLIB was successfully assigned as follows:
      Engine:        GREENPLM
      Physical Name: XXX.XXX.XXX.XXX
*/

* Clear libname;
libname mydblib clear;

/*
NOTE: Libref MYDBLIB has been deassigned.
*/

/* *********** Connecting to Greenplum/HAWQ (SQL Pass-through) ********** */

* SQL Pass-through facility;
proc sql;
  * Open connection to the database;
  CONNECT TO greenplm AS gpcon (server=&server. db=&database. port=&port. user=&user. password=&password.);

  * Close open connection;
  DISCONNECT FROM gpcon;
quit;

/*
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.43 seconds
      cpu time            0.00 seconds
*/

/* *********** Libname Options ********** */

* Libname option: DBCOMMIT;
libname mydblib greenplm server=&server. database=&database. port=&port. user=&user. password=&password. schema=&schema.
	dbcommit=100;

/*
NOTE: Libref MYDBLIB was successfully assigned as follows:
      Engine:        GREENPLM
      Physical Name: XXX.XXX.XXX.XXX
*/

* Deassigned libref;
libname mydblib clear;

* Libname option: AUTOCOMMIT;
libname mydblib greenplm server=&server. database=&database. port=&port. user=&user. password=&password. schema=&schema.
	autocommit=NO;

/*
NOTE: Libref MYDBLIB was successfully assigned as follows:
      Engine:        GREENPLM
      Physical Name: XXX.XXX.XXX.XXX
*/

* Deassigned libref;
libname mydblib clear;

* Libname option: QUERY_TIMEOUT;
libname mydblib greenplm server=&server. database=&database. port=&port. user=&user. password=&password. schema=&schema.
	query_timeout=120;

/*
NOTE: Libref MYDBLIB was successfully assigned as follows:
      Engine:        GREENPLM
      Physical Name: XXX.XXX.XXX.XXX
*/

* Deassigned libref;
libname mydblib clear;

/* *********** System Options ********** */

* Libname for options testing;
libname mydblib greenplm server=&server. database=&database. port=&port. user=&user. password=&password. schema=&schema.;

* Copy sashelp dataset 'cars' for testing;
data mydblib.cars;
  set sashelp.cars (obs=1);
run;

/*
NOTE: SAS variable labels, formats, and lengths are not written to DBMS tables.
NOTE: There were 1 observations read from the data set SASHELP.CARS.
NOTE: The data set MYDBLIB.CARS has 1 observations and 15 variables.
NOTE: DATA statement used (Total process time):
      real time           32.81 seconds
      cpu time            0.03 seconds
*/

* System option: STSUFFIX;
options sastrace=',,,d' sastraceloc=saslog STSUFFIX;
proc print data=mydblib.cars;
run;

/*
GREENPL: AUTOCOMMIT turned ON for connection id 0 0 1756512486 tkvercn1 0 PRINT
  1 1756512486 tkvercn1 0 PRINT
GREENPL_1: Prepared: on connection 0 2 1756512486 tkvercn1 0 PRINT
SELECT * FROM sasgpdb.CARS FOR READ ONLY  3 1756512486 tkvercn1 0 PRINT
  4 1756512486 tkvercn1 0 PRINT
32   run;

  5 1756512489 tkvercn1 0 PRINT
GREENPL_2: Executed: on connection 0 6 1756512489 tkvercn1 0 PRINT
Prepared statement GREENPL_1 7 1756512489 tkvercn1 0 PRINT
  8 1756512489 tkvercn1 0 PRINT
*/

* System option: NOSTSUFFIX;
options sastrace=',,,d' 	sastraceloc=saslog NOSTSUFFIX;
proc print data=mydblib.cars;
run;

/*
GREENPL_3: Prepared: on connection 0
SELECT * FROM sasgpdb.CARS FOR READ ONLY

34   proc print data=mydblib.cars;
35   run;


GREENPL_4: Executed: on connection 0
Prepared statement GREENPL_3
*/

* System option: SQLGENERATION;
* Print default values to log;
options sqlgeneration='';
proc options option=sqlgeneration;
run;

/*
    SAS (r) Proprietary Software Release 9.4  TS1M0

 SQLGENERATION=(NONE DBMS='TERADATA DB2 ORACLE NETEZZA ASTER GREENPLM HADOOP')
                   Specifies whether and when SAS procedures generate SQL for in-database
                   processing of source data.
*/

* Exclude in-database processing in Hadoop and PROC FREQ with Greenplum;
options sqlgeneration=(DBMS EXCLUDEDB='HADOOP' EXCLUDEPROC="GREENPLM='FREQ'");
proc options option=sqlgeneration;
run;

/*
    SAS (r) Proprietary Software Release 9.4  TS1M0

 SQLGENERATION=(DBMS EXCLUDEPROC="GREENPLM='FREQ'" EXCLUDEDB='HADOOP')
                   Specifies whether and when SAS procedures generate SQL for in-database
                   processing of source data.
*/


/* *********** MACRO Variables ********** */

* Copy sashelp dataset 'cars' for testing;
data mydblib.class;
  set sashelp.class;
run;

* SYSDBMSG;
%put %superq(SYSDBMSG);

/*
GREENPLUM: ERROR: The GREENPLUM table CLASS has been opened for OUTPUT. This table already
exists, or there is a name conflict with an existing object. This table will not be replaced.
This engine does not support the REPLACE option.
*/

data _null_;
  msg=symget("SYSDBMSG");
  put msg;
run;

* SYSDBRC;
%put %superq(SYSDBRC);

/*
-1
*/

data _null_;
  msg=symget("SYSDBRC");
  put msg;
run;

* SQLXMSG;
%put %superq(SQLXMSG);

/*
WARNING: Apparent symbolic reference SQLXMSG not resolved.
*/

data _null_;
  msg=symget("SQLXMSG");
  put msg;
run;

* SQLXRC;
%put %superq(SQLXRC);

/*
WARNING: Apparent symbolic reference SQLXRC not resolved.
*/

data _null_;
  msg=symget("SQLXRC");
  put msg;
run;

* Deassigned libref;
libname mydblib clear;
