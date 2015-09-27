package com.github.randerzander;

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.phoenix.spark._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.sql.Connection
import java.sql.DriverManager

object HiveToPhoenix{
  def main(args: Array[String]) {
    val zkUrl = args(0)
    val table = args(1)
    val pk = args(2)

    val sparkConf = new SparkConf().setAppName("HiveToPhoenix-"+table)
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    /*
    val query = "select * from test"
    var connection:Connection = null
      try {
        Class.forName("org.apache.hive.jdbc.HiveDriver")
        connection = DriverManager.getConnection("jdbc:hive2://localhost:10000", "", "")
        connection.createStatement().execute(query)
      } catch { case e: Throwable => e.printStackTrace }
    connection.close()
    */

    // Create a dataframe from source table. Capitalize all column names
    var df = sqlContext.sql("select * from " + table)
    df = df.toDF(df.columns.map(x => x.toUpperCase):_*)

    // Create the target Phoenix table if it doesn't already exist
    val command = "create table if not exists " + table + "(" + df.columns.map(x => x + " varchar,").mkString("") + " constraint my_pk primary key (" + pk +"))"
    var connection:Connection = null
      try {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
        connection = DriverManager.getConnection("jdbc:phoenix:"+zkUrl, "", "")
        connection.createStatement().execute(command)
      } catch { case e: Throwable => e.printStackTrace }
    connection.close()

    df.save("org.apache.phoenix.spark", SaveMode.Overwrite, Map("table" -> table, "zkUrl" -> zkUrl))

    sc.stop()
  }
}
