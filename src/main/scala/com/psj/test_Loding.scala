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
  val selloutDataFromOracle= spark.read.format("jdbc").
    options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

  // 메모리 테이블 생성
  selloutDataFromOracle.createOrReplaceTempView("selloutTable2")
  selloutDataFromOracle.show()

// Join
  spark.sql("select a.regionid, a.product, a.yearweek, a.qty, b.regionname " +
    "from selloutTable a " +
    "inner join selloutTable2 b " + //"left join selloutTable2 b "
    "on a.regionid = b.regionid")
//join
  var leftJoinData = spark.sql("select a.regionid, a.product, a.yearweek, a.qty, b.regionname " +
    "from selloutTable a left outer join selloutTable2 b " +
    "on a.regionid = b.regionid")


  //테이터 저장
  var myUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"

  // 데이터 저장
  val prop = new java.util.Properties
  prop.setProperty("driver", "oracle.jdbc.OracleDriver")
  prop.setProperty("user", "kopo")
  prop.setProperty("password", "kopo")
  val table = "test111"
  //append
  leftJoinData.write.mode("overwrite").jdbc(myUrl, table, prop)

  

  // 파일저장
  leftJoinData.
    coalesce(1). // 파일개수
    write.format("csv").  // 저장포맷
    mode("overwrite"). // 저장모드 append/overwrite
    option("header", "true"). // 헤더 유/무
    save("C:/spark/bin/data/leftJoinData.csv") // 저장파일명

}
