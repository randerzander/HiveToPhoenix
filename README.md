Apache Hive is a data warehouse style SQL engine for Apache Hadoop. Apache Phoenix is a SQL interface on top of Apache HBase. While Hive is great at executing batch and large-scale analytical queries, HBase's indexed lookups will be faster for record counts of up to a few million rows (specific count depends on a variety of factors).

The HiveToPhoenix artifact is intended to be used as a reusable application for executing queries against Hive tables and saving results to Phoenix tables. It requires an input properties file and relies on SparkSQL's DataFrames to make Hive->Phoenix data movement easy.

Job Properties file must have the following format:
```
srcUser=
srcPass=
srcClass=org.apache.hive.jdbc.HiveDriver
srcConnStr=jdbc:hive2://localhost:10000
srcTable=test
srcScript=test/test_in.sql

dstTable=test
dstUser=
dstPass=
dstClass=org.apache.phoenix.jdbc.PhoenixDriver
dstConnStr=jdbc:phoenix:localhost:2181:/hbase-unsecure
dstZkUrl=localhost:2181:/hbase-unsecure
dstPk=id

typeMap=string|varchar,int|integer
```

If srcScript is null, the Phoenix table will be loaded with the result of "select * from srcTable".

If srcScript is not null, the contents of the file at the specified path will be executed by SparkSQL and results will be loaded.

Note: srcScript is provided as a convenience the allows the user to specify filters, but the query results currently must have the same schema as srcTable.

Example:
```
hadoop fs -put test/input/ .
hive --hiveconf PATH=/user/root/input/ -f test/test.ddl
export HADOOP_CONF_DIR=/etc/hadoop/conf
$SPARK_HOME/bin/spark-submit --class com.github.randerzander.HiveToPhoenix --master yarn-client --num-executors 1 --executor-memory 512M HiveToPhoenix-0.0.1-SNAPSHOT.jar job.props
/usr/hdp/current/phoenix-client/bin/psql.py localhost:/hbase-unsecure test/test_out.sql

Output:
ID                                                                            VAL 
---------------------------------------- ---------------------------------------- 
a                                                                               1 
Time: 0.039 sec(s)
```

ToDo:
1. Support derived expressions, e.g. select id, count(*) as count from srcTable

To build manually:
```
mvn clean package
```
