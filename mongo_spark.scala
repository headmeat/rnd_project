/*RnD 개발*/

//spark 접속
spark-shell --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.1


//MongoDB에서 불러오기

import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.sql._
import org.bson.Document
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.sql.Timestamp
import java.time.Duration
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Column

import org.apache.log4j.Logger
import org.apache.log4j.Level


val base ="mongodb://127.0.0.1/cpmongo."

val replyUri = "CPS_BOARD_REPLY"  //댓글
val codeUri = "CPS_CODE_MNG"  //통합 코드관리 테이블
val gradCorpUri = "CPS_GRADUATE_CORP_INFO"  //졸업 기업
val ncrInfoUri = "CPS_NCR_PROGRAM_INFO"  //비교과 정보
val ncrStdInfoUri = "CPS_NCR_PROGRAM_STD"  //비교과 신청학생
val outActUri = "CPS_OUT_ACTIVITY_MNG"  //교외활동
val jobInfoUri = "CPS_SCHOOL_EMPLOY_INFO"  //채용정보-관리자 등록
val sjobInfoUri = "CPS_SCHOOL_EMPLOY_STD_INFO"  //채용정보 신청 학생 정보(student job info)


val deptInfoUri = "V_STD_CDP_DEPT"  //학과 정보 (department info)
val clPassUri = "V_STD_CDP_PASSCURI" //교과목 수료(class pass)
val stInfoUri = "V_STD_CDP_SREG"  //학생 정보 (student info)
val pfInfoUri = "V_STD_CDP_STAF"  //교수 정보 (professor info)
val clInfoUri = "V_STD_CDP_SUBJECT"  //교과 정보 (class info)

Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)
Logger.getLogger("MongoRelation").setLevel(Level.OFF)
Logger.getLogger("MongoClientCache").setLevel(Level.OFF)

def getMongoDF(
 spark : SparkSession,
 coll : String ) : DataFrame = {
   spark.read.mongo(ReadConfig(Map("uri"->(base+coll))))
}

def setMongoDF(
spark : SparkSession,
coll: String,
df : DataFrame ) = {
df.saveToMongoDB(WriteConfig(Map("uri"->(base+coll))))
}
//------------------------------------------------------------------------------
//mongodb 안켜져있으면  cd $SPARK_HOME에서 mongod 하고 다른 쉘에서 같은 위치로 이동하고 mongo 하고 또 다른 쉘에서 다시 spark 실행

//학교 구분해서 가져옴
val university = getMongoDF(spark, clInfoUri)
val univDF = university.toDF
val univAll = univDF.select(col("COLG_CD_NM"))
val univDistinct = univAll.distinct
univDistinct.show

//------------------------------------------------------------------------------
//1.학교 별 학과 : 학생 정보 테이블에서 대학코드 1개로 학과 select
// val departmentInfo = university.select(col("SUST_CD_NM"))
// val departDistinct = departmentInfo.distinct
// departDistinct.show

// db.collection이름.find()
//
// [테이블]
// 학생정보 V_STD_CDP_SREG
// 비교과정보 CPS_NCR_PROGRAM_INFO
// 비교과 신청 학생 CPS_NCR_PROGRAM_STD
// 교과정보 V_STD_CDP_SUBJECT
// 교과목 수료 V_STD_CDP_PASSCURI
// 교외활동 CPS_OUT_ACTIVITY_MNG


// COLG_CD(대학코드), COLG_CD_NM(대학명)
// 01, 공학부
// 02, 디자인예술학부
// 03, 어문사회학부
// 월요일에 보경언니랑 임박사님께 데이터 다시 확인해보고
// 일단 이거로 긁어오기
//
// 대학교 별 학과 긁어오기
//from 학생정보 테이블
val studentInfoTable = getMongoDF(spark, stInfoUri)
studentInfoTable.show
val univAndDepart = studentInfoTable.select(col("COLG_CD"), col("COLG_CD_NM"), col("SUST_CD"), col("SUST_CD_NM"))
univAndDepart.show


val mechanic_01 = univAndDepart.where($"COLG_CD"===1)
val language_02 = univAndDepart.where($"COLG_CD"===2)
val design_03 = univAndDepart.where($"COLG_CD"===3)

//학교 별 학과 고유하게 출력
val mechanic = mechanic_01.distinct
val language = language_02.distinct
val design = design_03.distinct
mechanic.show
language.show
design.show


//------------------------------------------------------------------------------
//2.학과 별 교과과목
//01.공학부 학생들이 수강한 교과정보
//from 교과정보 테이블
val classInfoTable = getMongoDF(spark, clInfoUri)
classInfoTable.show

val classInfo = classInfoTable.select(col("COLG_CD"), col("COLG_CD_NM"), col("SBJT_KOR_NM"))
classInfo.show

//학과아이디? 학교아이디가 이건 또 많음 
val colg_cd = classInfoTable.select(col("COLG_CD"))
val colg_cd_distinct = colg_cd.distinct
colg_cd_distinct.show


val classInfoFromMechanic = classInfo.where($"COLG_CD"==="00001")
val classInfoFromMechanicDistinct = classInfoFromMechanic.distinct
classInfoFromMechanicDistinct.show(30)


val classInfoFromMechanic = classInfo.where($"COLG_CD"==="00002")
val classInfoFromMechanicDistinct = classInfoFromMechanic.distinct
classInfoFromMechanicDistinct.show(30)



//------------------------------------------------------------------------------
//3.학과 별 비교과 과목



//------------------------------------------------------------------------------
//4.학과 별 자율활동









// val test = getMongoDF(spark, jobInfoUri)
// test.show(10)

// val university = getMongoDF(spark, clInfoUri)
// university.show(20)

// val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()


// import spark.implicits._

// val test = spark.createDataFrame(university)
// test.printScheme()

// val university = getMongoDF(spark, clInfoUri)
// val univDF = university.toDF
// univDF.select(col("COLG_CD_NM")).show()
