package com.epam.spark.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object UniqueProductLocation {
  System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
  System.setProperty("HADOOP_HOME", "C:\\hadoop\\bin\\winutils.exe")
  val AGGREGATED_DIR: String = "aggregated"
  def main(args: Array[String]): Unit = {

    val demographicPath = args(0)
    val transcationPath = args(1)
    val outputBasePath = args(2)

    val sc = new SparkContext(new SparkConf().setAppName("UniqueProductLocation-recommendation").setMaster("local[*]"))
    val sqlContext = new HiveContext(sc)

    val demographic = sqlContext.read
      .format("com.databricks.spark.csv")
      .load(demographicPath).toDF("UserId","emailID","Location")

    val transaction = sqlContext.read
      .format("com.databricks.spark.csv")
      .load(transcationPath)
      .toDF("TransactionId",
        "ProductId",
        "UserId",
        "PurchaseAmount",
        "ItemDescription")

    val filteredTotalJoinexchangeRates = demographic.join(transaction, "UserId")
      .selectExpr("Location", "UserId", "ProductId", "ItemDescription")

    val uniqueProductLocation = filteredTotalJoinexchangeRates
      .groupBy("ProductId","ItemDescription","Location")
      .count().selectExpr("ProductId","ItemDescription","Location")

    uniqueProductLocation.coalesce(1).write
      .format("com.databricks.spark.csv")
      .save(s"$outputBasePath/$AGGREGATED_DIR")
  }

}
