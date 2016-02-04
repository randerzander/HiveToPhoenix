Apache Hive is a data warehouse style SQL engine for Apache Hadoop. Apache Phoenix is a SQL interface on top of Apache HBase. While Hive is great at executing batch and large-scale analytical queries, HBase's indexed lookups will be faster for record counts of up to a few million rows (specific count depends on a variety of factors).

The HiveToPhoenix artifact is intended to be used as a reusable application for executing queries against Hive tables and saving results to Phoenix tables (and vice versa). It requires an input properties file (job.properties, following typical key=value format) and relies on SparkSQL's DataFrames to make data movement easy.

***Saving Hive queries and tables into Phoenix:***

**Example job.properties file:** Runs 2 Hive queries and saves results + an additional table into Phoenix:
```
srcScripts=test/one.sql,test/two.sql
srcTables=three

dstTables=one,two,three
zkUrl=localhost:2181:/hbase-unsecure
dstPk=id

typeMap=string|varchar,int|integer
destination=phoenix
```

srcScripts is a comma separated list of local files which will be executed as SparkSQL queries.

srcTables is a comma separated list of Hive tables which will be queried as "select * from table".

dstTables is a comma separated list of Phoenix table names which should be populated from srcScript queries/srcTables.

Make sure that dstTables entries are ordered with destination table names for srcScripts query results first, followed by destination tablenames corresponding to srcTables entries.

In the properties example above, we generate query results for scripts test/one.sql and test/two.sql. These results are loaded into Phoenix destination tables "one", and "two". Finally, we execute "select * from three" and load results into Phoenix destination table "three".

typeMap exists because Hive types and Phoenix types are not 1 to 1. "string|varchar" means that "string" types in the source table will be mapped to "varchar" types in the Phoenix table. Hive and Phoenix types are evolving, so the user is free to update this typeMap field.

The "jars" property allows end-users to supply a comma separated list of jars which need to be available on the classpath of the executors (JVM libraries, etc)

***Saving Phoenix tables into Hive:***

To query Phoenix tables and save them back to Hive, change "destination=phoenix" to "destination=hive". When saving tables to Hive, "format" specifies the intended file-format (ORC, Parquet, Avro, text, etc).

**WARNING:** Due to SparkSQL API changes in Spark 1.5.2, copying from Phoenix to Hive only works with Spark 1.4.1 or lower. See [this gist](https://gist.github.com/randerzander/cbcf30f2db67d9a6fd57) for instructions on downloading and using Apache Spark 1.4.1 with HDP 2.3.4.

**Example job.properties file:** Copy a Phoenix table back to Hive:
```
srcTables=phoenix_table

dstTables=phoenix_table_copy
zkUrl=localhost:2181:/hbase-unsecure

destination=hive
format=orc
```

**Note**: The following example assumes there are no existing Hive or Phoenix tables named "test".
**Full Example**:
```
hadoop fs -put test/input/ .
hive --hiveconf PATH=/user/root/input/ -f test/test.ddl
export SPARK_HOME=/usr/hdp/current/spark-client
export HADOOP_CONF_DIR=/etc/hadoop/conf
$SPARK_HOME/bin/spark-submit --class com.github.randerzander.HiveToPhoenix --master yarn-client target/HiveToPhoenix-0.0.1-SNAPSHOT.jar test/job.props
/usr/hdp/current/phoenix-client/bin/psql.py localhost:/hbase-unsecure test/test_out.sql

Output:
ID                                                                            VAL 
---------------------------------------- ---------------------------------------- 
a                                                                               1 
Time: 0.039 sec(s)
```

Here we've put data in HDFS & defined three Hive tables for reading it. Then we ran a Spark job to load all three Hive tables into equivalent Phoenix tables.

test/input/test.txt contained 2 records, but the Phoenix tables contains only one: the test/one.sql and test/two.sql scripts included "where" clause filters, demonstrating the ability to load specific subsets of records.


To build manually, make sure JAVA_HOME is pointing to a Java 1.7 JDK and:
```
mvn clean package
```
