
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


//토드조인
//  SELECT A.REGIONID, B.REGIONNAME, A.PRODUCT, A.YEARWEEK, A.QTY
 // FROM KOPO_CHANNEL_SEASONALITY_NEW A
 // LEFT JOIN KOPO_REGION_MST B
 //   ON A.REGIONID = B.REGIONID

//토드 3테이블 조인
// select A.*
// ,B.productname
// ,c.promotion_ratio
// ,d.promotion_Data
// FROM lefttable A
// LEFT JOIN righttable B
//  ON A.productgroup = B.productid
// LEFT JOIN table2 C
//   ON A.productgroup = c.productgroup
// AND A.yearweek = C.yearweek
// LEFT JOIN table3 D
//  ON A.productgroup = D.productgroup


//  select A.*
//  ,B.productname
//  ,c.promotion_ratio
//  ,d.promotion_Data

//  FROM lefttable a,
//  righttable b,
//  table2 c,
//  table3 d
//    where 1=1
//  And A.productgroup = B.productid (+)
//  AND A.productgroup = c.productgroup (+)
//  AND A.yearweek = C.yearweek (+)
//  AND A.productgroup = D.productgroup (+);
}