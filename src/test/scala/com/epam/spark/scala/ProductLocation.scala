package com.epam.spark.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object ProductLocation {


  System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
  System.setProperty("HADOOP_HOME", "C:\\hadoop\\bin\\winutils.exe")

  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {

    require(args.length == 3, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val demographicPath = "D:\\Assignment\\On-Boarding\\src\\test\\input\\demographic_info.txt"
    val transcationPath = args(1)
    val outputBasePath = args(2)

    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation").setMaster("local[*]"))
    val sqlContext = new HiveContext(sc)


    processData(sqlContext, demographicPath, transcationPath,outputBasePath)
    sc.stop()
  }

  def processData(sqlContext: HiveContext, demographicPath: String, transcationPath: String,outputBasePath :String) ={

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawDemographic : DataFrame = getRawData(sqlContext, demographicPath)
    val rawTransaction : DataFrame = getRawData(sqlContext, transcationPath)

    println("Transactions:")
    rawTransaction.show
    println("Demographic:")
    rawDemographic.show

    val uniqueLocation: DataFrame = getUniqueLocation(rawDemographic, rawTransaction)

    uniqueLocation.write
      .format("com.databricks.spark.csv")
      .save(s"$outputBasePath/$AGGREGATED_DIR")


  }

def getUniqueLocation(rawDemographic : DataFrame,rawTransaction :  DataFrame):DataFrame ={

  rawDemographic
}
  def getRawData(sqlContext: HiveContext, dataPath: String): DataFrame = {

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .load(dataPath)
    df
  }
}
