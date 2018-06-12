package com.psj

import .mapRdd
import edu.princeton.cs.introcs.StdStats
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

object 표준편차 {

  val spark = SparkSession.builder().appName("hkProject").
    config("spark.master", "local").
    getOrCreate()


  var MAXVALUE = 700000

  /////////////////////////////

  // oracle connection
  var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"

  var staticUser = "kopo"
  var staticPw = "kopo"
  var selloutDb = "kopo_channel_seasonality_new"
  var productNameDb = "kopo_product_mst"

  val selloutDf = spark.read.format("jdbc").
    options(Map("url" -> staticUrl, "dbtable" -> selloutDb,
      "user" -> staticUser,
      "password" -> staticPw)).load

  val productMasterDf = spark.read.format("jdbc").
    options(Map("url" -> staticUrl, "dbtable" -> productNameDb,
      "user" -> staticUser,
      "password" -> staticPw)).load

  selloutDf.createOrReplaceTempView("selloutTable")
  productMasterDf.createOrReplaceTempView("mstTable")

  var rawData = spark.sql("select " +
    "concat(a.regionid,'_',a.product) as keycol, " +
    "a.regionid as accountid, " +
    "a.product, " +
    "a.yearweek, " +
    "cast(a.qty as double) as qty, " +
    "b.product_name " +
    "from selloutTable a " +
    "left join mstTable b " +
    "on a.product = b.product_id")

  rawData.show(2)

  var rawDataColumns = rawData.columns
  var keyNo = rawDataColumns.indexOf("keycol")
  var accountidNo = rawDataColumns.indexOf("accountid")
  var productNo = rawDataColumns.indexOf("product")
  var yearweekNo = rawDataColumns.indexOf("yearweek")
  var qtyNo = rawDataColumns.indexOf("qty")
  var productnameNo = rawDataColumns.indexOf("product_name")

  // (kecol, accountid, product, yearweek, qty, product_name)
  var rawRdd = rawData.rdd

  var filteredRdd = rawRdd.filter(x=>{
    // boolean = true
    var checkValid = true
    // 찾기: yearweek 인덱스로 주차정보만 인트타입으로 변환
    var weekValue = x.getString(yearweekNo).substring(4).toInt

    // 비교한후 주차정보가 53 이상인 경우 레코드 삭제
    if( weekValue >= 53){
      checkValid = false
    }

    checkValid
  })
  // filteredRdd.first
  // filteredRdd = (키정보, 지역정보, 상품정보, 연주차정보, 거래량 정보, 상품이름정보)

  // 처리로직 : 거래량이 MAXVALUE 이상인건은 MAXVALUE로 치환한다.


  var mapRdd = filteredRdd.map(x=>{

    // 디버깅코드: var x = mapRdd.filter(x=>{ x.getDouble(qtyNo) > 700000 }).first
    //로직구현예정

    var org_qty = x.getDouble(qtyNo)
    var new_qty = org_qty

    if(new_qty > MAXVALUE){
      new_qty = MAXVALUE
    }

    //출력 row 키정보, 연주차정보, 거래량 정보_org, 거래량 정보_new )
    Row( x.getString(keyNo),
      x.getString(yearweekNo),
      org_qty,
      new_qty
    )
  })

  //그룹

  var groupRddMap = mapRdd.
    groupBy(x=>{ (x.getString(keyNo)) }).
    map(x=>{
      var key = x._1   //1
      var data = x._2   //156 배열(  , , , , ,   )  * 156.toArray.foreach(println)
      var datasize = data.size
      var sum = data.map(x=>{x.getDouble(2)}).sum

      var average = 0.0d
      if(datasize!=0){
        average = sum/datasize
      }else{
        average = 0.0
      }

      var sum2 = data.map(x=>math.pow(({x.getDouble(2)}-average),2)).sum
      var average2 = 0.0d
      if(datasize!=0){
        average2 = sum2/datasize
      }else{
        average2 = 0.0
      }
      var std = math.sqrt(average2)

      (
        key,
        (sum,
        datasize,
        average,
        std)
      )  // a01, product1, 평균 10
    })
  // 12만건 / 156




  var groupRddMap2 = mapRdd.
    groupBy(x=>{ (x.getString(keyNo)) }).
    flatMap(x=>{
      var key = x._1
      var data = x._2
      var datasize = data.size
      var sum = data.map(x=>{x.getDouble(2)}).sum
      var qty = data.map(x=>{x.getDouble(qtyNo)}).toArray

      var average = 0.0d
      if(datasize!=0){
        average = sum/datasize
      }else{
        average = 0.0
      }



      //평균계산
      var averag2 = StdStats.mean(qty)

      //표준편차
      var stddev = StdStats.stddev(qty)

      //결과출력 //  156건
      var ratio = 1.0d
      var mapResult = data.map(x=>{
        ratio = x.getDouble(qty)/averag2
        (
          key,
          datasize,
          average,
          stddev,
          sum,
        )
      })
      mapResult
    })








  // 12만건 / 156

  //  var groupRddMap3 = mapRdd.
  //    groupBy(x=>{ (x.getString(keyNo)) }).
  //    map(x=>{
  //      var key = x._1
  //      var data = x._2
  //      var datasize = data.map(x=>{x.getDouble(qtyNo)}).size
  //      var sum = x._2.map(x=>{x.getDouble(qtyNo)}).sum
  //      var average = 0.0d
  //      if(datasize!=0){
  //        average = sum/datasize
  //      }else{
  //        average = 0.0
  //      }
  //      var ratio = 0.0d
  //      var mapResult = data.map(x=>{
  //        ratio = x.getDouble(qtyNo)/average
  //        (
  //          key,
  //          datasize,
  //          average,
  //          ratio
  //        )
  //      })
  //      mapResult
  //    })

  //  var groupRddMap = mapRdd.
  //    groupBy(x=>{ (x.getString(keyNo)) }).
  //    flatMap(x=>{
  //      var key = x._1
  //      var data = x._2
  //      var datasize = data.map(x=>{x.getDouble(qtyNo)}).size
  //      var sum = x._2.map(x=>{x.getDouble(qtyNo)}).sum
  //      var average = 0.0d
  //      if(datasize!=0){
  //        average = sum/datasize
  //      }else{
  //        average = 0.0
  //      }
  //      var ratio = 0.0d
  //      var mapResult = data.map(x=>{
  //        ratio = x.getDouble(qtyNo)/average
  //        (key,
  //          x.getString(accountidNo),
  //          x.getString(productNo),
  //          x.getString(yearweekNo),
  //          x.getDouble(qtyNo),
  //          average,
  //          ratio
  //        )
  //      })
  //      mapResult
  //    })
}