package com.github.randerzander;

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.sql.{Connection, DriverManager, DatabaseMetaData, ResultSet}
import scala.collection.mutable.HashMap
import scala.io.Source.fromFile

import org.apache.phoenix.spark._

object HiveToPhoenix{
  def main(args: Array[String]) {
    val props = getProps(args(0))

    val srcUser = props get "srcUser" get
    val srcPass = props get "srcPass" get
    val srcClass = props get "srcClass" get
    val srcConnStr = props get "srcConnStr" get
    val srcTable = props get "srcTable" get
    val srcScript = props get "srcScript" get

    val dstUser = props get "dstUser" get
    val dstPass = props get "dstPass" get
    val dstClass = props get "dstClass" get
    val dstConnStr = props get "dstConnStr" get
    val dstTable = props get "dstTable" get
    val dstPk = props get "dstPk" get
    val dstZkUrl = props get "dstZkUrl" get

    // Establish src->dst type mapping
    var typeMap = new HashMap[String, String]().withDefaultValue(null)
    props.get("typeMap").get.split(",").map(x => typeMap.put(x.split("\\|")(0).toLowerCase, x.split("\\|")(1).toLowerCase))

    val srcMeta = getConn(srcClass, srcConnStr, srcUser, srcPass).getMetaData()
    val srcTableMeta = srcMeta.getColumns(null, null, srcTable, null)

    // Create Phoenix DDL
    var command = "create table if not exists " + dstTable + "("
    while (srcTableMeta.next){
      val colName = srcTableMeta.getString(4)
      val srcType = srcTableMeta.getString(6).toLowerCase
      val dstType = if (typeMap contains srcType) typeMap get srcType get else srcType
      command += colName + " " + dstType + ","
    }
    command += " constraint my_pk primary key (" + dstPk +"))"
    println("Phoenix DDL Statement:\n" + command)

    // Execute Phoenix DDL
    getConn(dstClass, dstConnStr, dstUser, dstPass).createStatement().execute(command)

    val query = if (srcScript != null) fromFile(srcScript).getLines().mkString("") else "select * from " + srcTable
    println("Hive query: \n" + query)

    // Load source into df and save to Phoenix
    val sparkConf = new SparkConf().setAppName("HiveToPhoenix-"+srcTable+"-"+dstTable)
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    var df = sqlContext.sql(query)
    df = df.toDF(df.columns.map(x => x.toUpperCase):_*)
    df.save("org.apache.phoenix.spark", SaveMode.Overwrite, Map("table" -> dstTable.toUpperCase, "zkUrl" -> dstZkUrl))

    sc.stop()
  }

  def getConn(driverClass: => String, connStr: => String, user: => String, pass: => String): Connection = {
    var conn:Connection = null
    try{
      Class.forName(driverClass)
      conn = DriverManager.getConnection(connStr, user, pass)
    }catch{ case e: Exception => e.printStackTrace }
    conn
  }

  def getProps(file: => String): HashMap[String,String] = {
    var props = new HashMap[String,String]
    val lines = fromFile(file).getLines
    lines.foreach(x => if (x contains "=") props.put(x.split("=")(0), if (x.split("=").size > 1) x.split("=")(1) else null))
    props
  }
}
