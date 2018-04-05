package com.psj

import org.apache.spark.sql.SparkSession


object Example3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    var priceData = Array("바나나킥", "새우깡", "간쵸", "빼빼로")
    var price = "KOPO_"
    var priceDataSize = priceData.size

    for (i <- 0 until priceDataSize) {
      //var i = 0
      var promotionEffect = price + priceData(i)
      priceData(i) = promotionEffect
    }

  }
}