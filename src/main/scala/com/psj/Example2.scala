package com.psj

import org.apache.spark.sql.SparkSession


object Example2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()
    // 조건 판단하기
    // 체크: 입력데이터의 연도 최대 값
    var currentYear = 2018
    var deltaYear = 0
    // 체크: 입력데이터의 연도 최소 값
    var validYear = 2015

    if (deltaYear != 0) {
      validYear = currentYear - deltaYear
    } else {
      validYear
    }

  }






















}