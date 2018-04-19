package com.psj

import com.psj.Concat.test_Loding.{middleResult, spark}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, DoubleType, StructField, StructType}//스파크에서도 실행


object RDD_EX {

  val spark = SparkSession.builder().appName("Concat").
    config("spark.master", "local").
    getOrCreate()

  //Oracle XE 데이터 불러오기
  // 접속정보 설정//
  var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
  var staticUser = "kopo"
  var staticPw = "kopo"
  var selloutDb = "kopo_channel_seasonality_new"
  var selloutDb1 ="kopo_product_master"

  // jdbc (java database connectivity) 연결
  val selloutDataFromOracle= spark.read.format("jdbc").
    options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load
  val selloutDataFromOracle1= spark.read.format("jdbc").
    options(Map("url" -> staticUrl,"dbtable" -> selloutDb1,"user" -> staticUser, "password" -> staticPw)).load

  // 메모리 테이블 생성
  selloutDataFromOracle.createOrReplaceTempView("selloutTable")
  selloutDataFromOracle.show(1)

  selloutDataFromOracle1.createOrReplaceTempView("selloutTable1")
  selloutDataFromOracle1.show(1)

  var mainData = selloutDataFromOracle.rdd
  var subData = selloutDataFromOracle1.rdd

////조인
  var middleResult = spark.sql("select " +
   // "concat(A.REGIONID,'_',A.PRODUCT) as keycol, " +
    "A.REGIONID, " +
    "A.PRODUCT, " +
    "A.YEARWEEK, " +
    "cast(A.QTY as double) as QTY, " +
    "B.PRODUCTNAME " +
    "from selloutTable a " +
    "left join selloutTable1 b "+
    "on A.PRODUCT = B.PRODUCTID")

  //인텍스 부여
  var rawDataColumns = middleResult.columns

  //var keycolNO = rawDataColumns.indexOf("keycol")
  var REGIONIDNO = rawDataColumns.indexOf("REGIONID")
  var PRODUCTNO = rawDataColumns.indexOf("PRODUCT")
  var YEARWEEKNO = rawDataColumns.indexOf("YEARWEEK")
  var QTYNO = rawDataColumns.indexOf("QTY")
  var PRODUCTNAMENO = rawDataColumns.indexOf("PRODUCTNAME")

  var rawRdd = middleResult.rdd

  //////////////////////////////////////////////////////////////////////
  // RDD-정제연산

  // 데이터형 변환 [데이터프레임 → RDD]
  //var {RDD변수명} = {RDD변수명}.filter(x=>{ 필터조건식})

  // 데이터 확인
  //var {RDD변수명}.collect.foreach(println)

  var filterexRdd = rawRdd.filter(x=> {
    // Boolean  = true
    var checkValid = true
   //찾기 : yearweek 인덱스로 주차정보만 인트타입으로 변환
    var weekValue = x.getString(YEARWEEKNO).substring(4).toInt

    //비교한후 주차정보가 53 인상인 경우 레코드 삭제
    if(weekValue >= 53){checkValid = false}
    checkValid
  })

  var rwaExRdd = rawRdd.filter(x=>{
    var checkValid = true
    if((x.getString(REGIONIDNO) == "A60") &&
      (x.getString(PRODUCTNO) == "PRODUCT34") &&
      (x.getString(YEARWEEKNO) == "201402")){
      checkValid = false
    }
    checkValid
  })
  var x = resultRdd.first

//////////////////////////////////////////////////////////////////////////////////////////
  var productArray = Array("PRODUCT1", "PRODUCT2")

  var productSet = productArray.toSet

  var resultRdd = filterexRdd.filter(x=> {
    var checkValid = false

    var productInfo = x.getString(PRODUCTNO);

   if(productSet.contains(productInfo)){//contains --> 포함의 여부 확인 메서드
     checkValid = true
   }
    checkValid
  })

  // 데이터 확인
  //var {resultRdd}.collect.foreach(println)
  resultRdd.collect.foreach(println)

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//RDD → Dataframe
  //var {DataFrame 변수명} = spark.createDataframe( {RDD명},
 //   StructType(Seq( StructField( “컬럼명#1”, 데이터타입#1),
 // StructField( “컬럼명#2”, 데이터타입#2))
  resultRdd

var productDataFrame = spark.createDataFrame(resultRdd,
  StructType(
    Seq(
      StructField("KEY", StringType),
      StructField("REGIONID", StringType),
      StructField("PRODUCT", StringType),
      StructField("YEARWEEK", StringType),
      StructField("VOLUME", DoubleType),
      StructField("PRODUCT_NAME", StringType))))






}
