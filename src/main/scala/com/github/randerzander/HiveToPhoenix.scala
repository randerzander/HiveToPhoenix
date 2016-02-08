package com.github.randerzander;

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.sql.{Connection, DriverManager}
import scala.collection.mutable.HashMap
import scala.io.Source.fromFile

object HiveToPhoenix{
  def main(args: Array[String]) {
    val props = getProps(args(0))

    val srcScripts = getArrayProp(props, "srcScripts")
    val srcTables = getArrayProp(props, "srcTables").map(t=>t.toUpperCase)
    val dstTables = getArrayProp(props, "dstTables").map(t=>t.toUpperCase)
    val pk = props.getOrElse("dstPk", None)

    val zkUrl = props.getOrElse("zkUrl", "localhost:2181:/hbase-unsecure")
    val destination = props.getOrElse("destination", "phoenix").toLowerCase()

    if (srcScripts.size + srcTables.size != dstTables.size){
      println(srcScripts.size+" srcScripts + "+srcTables.size+" srcTables not equal to "+dstTables.size+" dstTables")
      System.exit(-1)
    }

    if (srcScripts.size > 0 && !destination.equals("phoenix")){
      println("SQL scripts for copying from Phoenix to Hive not supported")
      System.exit(-1)
    }

    val format = props.getOrElse("format", "orc")
    val jdbcClass = "org.apache.phoenix.jdbc.PhoenixDriver"
    val connStr = "jdbc:phoenix:" + zkUrl
    val jars = getArrayProp(props, "jars")

    // Establish src->dst type mapping
    var typeMap = new HashMap[String, String]().withDefaultValue(null)
    getArrayProp(props, "typeMap").map(x => typeMap.put(x.split("\\|")(0).toLowerCase, x.split("\\|")(1).toLowerCase))

    // Create SparkContext
    val sparkConf = new SparkConf().setAppName("HiveToPhoenix")
    val sc = new SparkContext(if (jars.size > 0) sparkConf.setJars(jars) else sparkConf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    // Build array of DataFrames for saving
    var dfs = Array[DataFrame]()
    val scripts = srcScripts.map(script => fromFile(script).getLines().mkString(""))
    scripts.map(q => dfs = dfs :+ sqlContext.sql(q.stripSuffix(";")))
    for(t <- srcTables){
      if (destination.equals("phoenix")) dfs = dfs :+ sqlContext.sql("select * from " + t)
      else dfs = dfs :+ sqlContext.load("org.apache.phoenix.spark", Map("table" -> t, "zkUrl" -> zkUrl))
    }

    for((df, i) <- dfs.zipWithIndex){
      println("INFO: Saving to "+destination+" table: " + dstTables(i))
      val tmpDf = df.toDF(df.columns.map(x => x.toUpperCase): _*)
      if (destination.equals("phoenix")){
        // Create DDL
        var command = "create table if not exists " + dstTables(i) + "("
        for (field <- df.schema) {
          val srcType = field.dataType.simpleString
          val dstType = typeMap.getOrElse(srcType, srcType)
          command += field.name + " " + dstType + ","
        }
        command += " constraint my_pk primary key ("+pk+"))"
        println("INFO: DESTINATION DDL:\n" + command)
        // Execute Phoenix DDL
        getConn(jdbcClass, connStr).createStatement().execute(command)
        tmpDf.write.format("org.apache.phoenix.spark").mode(SaveMode.Overwrite).options(Map("table" -> dstTables(i), "zkUrl" -> zkUrl)).save()
      }
      else {
        // Workaround for Phoenix-2287 using Apache Spark 1.4.1
        tmpDf.registerTempTable(dstTables(i) + "_tmp")
        try {   
          sqlContext.sql("drop table if exists " + dstTables(i))
        } catch { 
          case _: Throwable => println("Table " + dstTables(i) + " didn't exist.. creating..")
        } 
        sqlContext.sql("create table `" + dstTables(i) + "` stored as " + format + " as select * from `" + dstTables(i) + "_tmp`")
        //tmpDf.write.format(format).mode(SaveMode.Overwrite).saveAsTable(dstTables(i))
      }
    }

    sc.stop()
  }

  def getConn(driverClass: => String, connStr: => String): Connection = {
    var conn:Connection = null
    try{
      Class.forName(driverClass)
      conn = DriverManager.getConnection(connStr, "", "")
    }catch{
      case e: Exception => {
        e.printStackTrace
        System.exit(1)
      }
    }
    conn
  }

  def getArrayProp(props: => HashMap[String,String], prop: => String): Array[String] = {
    return props.getOrElse(prop, "").split(",").filter(x => !x.equals(""))
  }

  def getProps(file: => String): HashMap[String,String] = {
    var props = new HashMap[String,String]
    val lines = fromFile(file).getLines
    lines.foreach(x => if (x contains "=") props.put(x.split("=")(0), if (x.split("=").size > 1) x.split("=")(1) else null))
    props
  }
}
