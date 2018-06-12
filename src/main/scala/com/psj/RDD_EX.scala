package com.psj

import com.psj.Concat.test_Loding.{middleResult, spark}
import com.psj.RDD_EX.{productArray, productSet}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}//스파크에서도 실행


object RDD_EX {

  val spark = SparkSession.builder().appName("Concat").
    config("spark.master", "local").
    getOrCreate()

  //Oracle XE 데이터 불러오기
  // 접속정보 설정//
  var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
  var staticUser = "kopo"
  var staticPw = "kopo"
  var MainData = "kopo_channel_seasonality_new"
  var SubData ="kopo_product_master"

  // jdbc (java database connectivity) 연결
  val MainDataFrom= spark.read.format("jdbc").
    options(Map("url" -> staticUrl,"dbtable" -> MainData,"user" -> staticUser, "password" -> staticPw)).load
  val SubDataFrom= spark.read.format("jdbc").
    options(Map("url" -> staticUrl,"dbtable" -> SubData,"user" -> staticUser, "password" -> staticPw)).load

  MainDataFrom.createOrReplaceTempView("MainTable")
  MainDataFrom.show(1)

  SubDataFrom.createOrReplaceTempView("SubTable")
  SubDataFrom.show(1)

  var mainData = MainDataFrom.rdd
  var subData = SubDataFrom.rdd

  ////조인
  var JoinResult = spark.sql("select " +
    "concat(A.REGIONID,'_',A.PRODUCT) as keycol, " +
    "A.REGIONID, " +
    "A.PRODUCT, " +
    "A.YEARWEEK, " +
    "A.QTY *1.2 as NEW_QTY, " +
    "B.PRODUCTNAME " +
    "from MainTable a " +
    "left join SubTable b "+
    "on A.PRODUCT = B.PRODUCTID")

  //인텍스 부여
  var rawDataColumns = JoinResult.columns

  var keycolNO = rawDataColumns.indexOf("keycol")
  var REGIONIDNO = rawDataColumns.indexOf("REGIONID")
  var PRODUCTNO = rawDataColumns.indexOf("PRODUCT")
  var YEARWEEKNO = rawDataColumns.indexOf("YEARWEEK")
  var QTYNO = rawDataColumns.indexOf("QTY")
  var PRODUCTNAMENO = rawDataColumns.indexOf("PRODUCTNAME")

  var rawRdd = JoinResult.rdd

  // 메모리 테이블 생성
  //////////////////////////////////////////////////////////////////////
  // RDD-정제연산

  // 데이터형 변환 [데이터프레임 → RDD]
  //var {RDD변수명} = {RDD변수명}.filter(x=>{ 필터조건식})
  var RDDRDD = rawRdd.filter(x=>{
    var checkValid = false
    var WEEK = x.getString(YEARWEEKNO).substring(4,6).toInt
    var YEAR = x.getString(YEARWEEKNO).substring(0,4).toInt
    if( YEAR >= 2016 &&
          WEEK != 53
    )
    {checkValid = true}
    checkValid
  })

  var RDDRDD2 = RDDRDD.filter(x=>{
    var checkValid = false
    if ((x.getString(PRODUCTNO)) == "PRODUCT1" ||
      (x.getString(PRODUCTNO)) == "PRODUCT2")
      {checkValid = true}
    checkValid
  })

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
    var checkValid = false
    if((x.getString(REGIONIDNO) == "A60") &&
      (x.getString(PRODUCTNO) == "PRODUCT4") &&
      (x.getString(YEARWEEKNO) == "201402")){
      checkValid = true
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
  mapRdd.collect.foreach(println)
  mapRdd.take(3)

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//RDD → Dataframe
  //var {DataFrame 변수명} = spark.createDataframe( {RDD명},
 //   StructType(Seq( StructField( “컬럼명#1”, 데이터타입#1),
 // StructField( “컬럼명#2”, 데이터타입#2))


var productDataFrame = spark.createDataFrame(resultRdd,
  StructType(
    Seq(
      StructField("KEY", StringType),
      StructField("REGIONID", StringType),
      StructField("PRODUCT", StringType),
      StructField("YEARWEEK", StringType),
      StructField("VOLUME", DoubleType),
      StructField("PRODUCT_NAME", StringType))))
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  //RDD-가공연산
  // key, account, product, yearweek, qty, productname
  var mapRdd = filterexRdd.map(x=>{
    var qty = x.getDouble(QTYNO).toDouble
    var maxValue = 700000
    if(qty > maxValue){qty = maxValue}
    Row( x.getString(keycolNO),
      x.getString(YEARWEEKNO),
      x.getString(QTYNO))
  })


  //디버깅

   //filterexRdd = [A02_PRODUCT6,A02,PRODUCT6,201403,0.0,LED_TV]
  //var mapRdd = filterexRdd
  //var x = mapRdd.first

  //처리 로직 MAXVALUE 이상인건은 MAXVALUE로 치환한다
  var MAXVALUE = 700000
  var mapRdd1 = filterexRdd.map(x=>{
    //디버깅 코드 : var x = mapRdd.filter(x=>{x.getDouble(QTYNO) > 700000 }).first
    //로직 구현 예정
    var org_qty = x.getDouble(QTYNO)
    var new_qty = org_qty
    if(new_qty > MAXVALUE){
      new_qty = MAXVALUE
    }
    //출력 row 정보 키정보, 지역정보, 상품정보, 연주차정보, 거래량 정보, 거재량 정보_NEW 상품이름 정보)
    Row(x.getString(keycolNO),
     x.getString(YEARWEEKNO),
     org_qty,
     new_qty
    )
   })













}
