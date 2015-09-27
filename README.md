Uses Spark to read Apache Hive tables and save them into Phoenix tables.

The specified Phoenix table will be created if it doesn't already exist (all columns will be varchars).

Build:
```
mvn clean package
```

Run:
```
cd $SPARK_HOME
./bin/spark-submit --class com.github.randerzander.HiveToPhoenix --master yarn-client HiveToPhoenix-0.0.1-SNAPSHOT.jar zookeeper_host:2181:/hbase-unsecure HiveTableName PhoenixPrimaryKey
```
