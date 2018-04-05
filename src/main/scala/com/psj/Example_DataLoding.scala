package com.psj

import org.apache.spark.sql.SparkSession

object Example_DataLoding {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    //Oracle XE 데이터 불러오기
      // 접속정보 설정//
         var staticUrl = "jdbc:oracle:thin:@192.168.110.4:1522/XE"
         var staticUser = "kopo"
         var staticPw = "kopo"
         var selloutDb = "KOPO_CHANNEL_SEASONALITY_ex"

      // jdbc (java database connectivity) 연결
         val selloutDataFromOracle= spark.read.format("jdbc").
         options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

      // 메모리 테이블 생성
         selloutDataFromOracle.createOrReplaceTempView("selloutTable")
         selloutDataFromOracle.show(1)


 //RDB(Postgres/greenplumDB) 불러오기
    // 접속정보 설정
        var staticUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
        var staticUser = "kopo"
        var staticPw = "kopo"
        var selloutDb = "kopo_batch_season_mpara"

    // jdbc (java database connectivity) 연결
        val selloutDataFromPg= spark.read.format("jdbc").
         options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
        selloutDataFromPg.createOrReplaceTempView("selloutTable")

//RDB(MySql) 불러오기
    // 파일설정
        var staticUrl = "jdbc:mysql://192.168.110.112:3306/kopo"
        var staticUser = "root"
        var staticPw = "P@ssw0rd"
        var selloutDb = "KOPO_PRODUCT_VOLUME"

    // jdbc (java database connectivity)
        val selloutDataFromMysql= spark.read.format("jdbc").
         options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

        selloutDataFromMysql.createOrReplaceTempView("selloutTable")





  }

}