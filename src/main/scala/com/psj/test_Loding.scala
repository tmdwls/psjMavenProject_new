package com.psj
import com.psj.DataJoin.spark
import org.apache.spark.sql.SparkSession;
object test_Loding {
  val spark = SparkSession.builder().appName("hkProject").
    config("spark.master", "local").
    getOrCreate()


  //Oracle 데이터 불러오기
  // 접속정보 설정//
  var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
  var staticUser = "kopo"
  var staticPw = "kopo"
  var selloutDb = "kopo_region_mst"

  // jdbc (java database connectivity) 연결
  val subData= spark.read.format("jdbc").
    options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

  // 메모리 테이블 생성
  subData.createOrReplaceTempView("subTable")
  subData.show()

// Join         "inner join mainTable a 작은 테이블이 우선 큰테이블이 앞"
  spark.sql("select a.REGIONID, a.PRODUCT, a.YEARWEEK, a.QTY, b.REGIONNAME " +
    "from mainTable a " +
    "left join subTable b " + //
    "on a.REGINOID = b.REGINOID")


//join
  var leftJoinData = spark.sql("select A.REGIONID, A.PRODUCT, A.YEARWEEK, A.QTY, B.REGIONNAME " +
    "from mainTable A left outer join subTable B " +
    "on A.REGIONID = B.REGIONID")
  //"select a.regionid, a.product, a.yearweek, a.qty, b.regionname "
  //left 자리만 inner 역시 작은테이블이 앞쪽

  //테이터 저장
  var myUrl = "jdbc:oracle:thin:@192.168.110.4:1522/XE"

  // 데이터 저장
  val prop = new java.util.Properties
  prop.setProperty("driver", "oracle.jdbc.OracleDriver")
  prop.setProperty("user", "tmdwls99")
  prop.setProperty("password", "tmdwls99")
  val table = "test123"
  //append
  leftJoinData.write.mode("overwrite").jdbc(myUrl, table, prop)



  // 파일저장
  leftJoinData.
    coalesce(1). // 파일개수
    write.format("csv").  // 저장포맷
    mode("overwrite"). // 저장모드 append/overwrite
    option("header", "true"). // 헤더 유/무
    save("C:/spark/bin/data/leftJoinData.csv") // 저장파일명


  ///////////// Oracle Express Usage ////////////////
  //alter system set processes=500 scope=spfile
  //show parameter processes
  //shutdown immediate
  //startup


}
