
package com.psj

import org.apache.spark.sql.SparkSession;

object DataJoin {

  val spark = SparkSession.builder().appName("...").
    config("spark.master", "local").
    getOrCreate()


  var dataPath = "c:/spark/bin/data/"
  var mainData = "kopo_channel_seasonality_ex.csv"
  var subData = "kopo_product_mst.csv"

  // Dataframe
  var mainDataDf = spark.read.format("csv").option("header", "true").load(dataPath + mainData)

  var subDataDf = spark.read.format("csv").option("header", "true").load(dataPath + subData)


  // Dataframe
  mainDataDf.createOrReplaceTempView("mainTable")

  subDataDf.createOrReplaceTempView("subTable")

  //a.productgroup b.productid
  spark.sql("select a.regionid, a.productgroup, b.productname, a.yearweek, a.qty " +
    "from mainTable a " +
    "left join subTable b " +
    "on a.productgroup = b.productid")


  var leftJoinData = spark.sql("select a.*, b.productname " +
    "from maindata a left outer join subdata b " +
    "on a.productgroup = b.productid")
}