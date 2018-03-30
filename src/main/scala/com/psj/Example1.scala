package com.psj



import org.apache.spark.sql.SparkSession
object Example1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()
    // 파일설정
    var staticUrl = "jdbc:mysql://192.168.110.112:3306/kopo"
    var staticUser = "root"
    var staticPw = "P@ssw0rd"
    var selloutDb = "KOPO_PRODUCT_VOLUME"

    // jdbc (java database connectivity)
    val selloutDataFromMysql= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromMysql.createOrReplaceTempView("selloutTable")
    selloutDataFromMysql.show(1)
  }

  var priceData = Array(1000.0,1200.0,1300.0,1500.0,10000.0)
  var promotionRate = 0.2
  var priceDataSize = priceData.size

  for(i <-0 until priceDataSize){
    var promotionEffect = priceData(i) * promotionRate
    priceData(i) = priceData(i) - promotionEffect
  }





}
