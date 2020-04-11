//-------------------- # # # 교과목 리스트 # # # --------------------------------
//--------------------from. 교과목수료 테이블 : 학과명, 학번, 학점----------------------
//<학과 DataFrame> : departDF / 전체 학과의 모든 학생
//###학과 별 학생 번호 보기 ###
var clPassUri_DF = clPassUri_table.select(col("SUST_CD_NM"), col("STD_NO")).distinct.toDF
clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo("컴퓨터공학과")).show


//@@@ 세무회계학과에서 개설된 수업이 없는 경우는 교과목리스트를 생성할 수 없음 => 교과목을 제외하고 유사도 비교를 수행해야 함 @@@
clPassUri_table.select(col("SUST_CD_NM"), col("SBJT_KOR_NM")).distinct.toDF.filter(clPassUri_table("SUST_CD_NM").equalTo("컴퓨터공학과")).show

//<학생 DataFrame> / 하나의 학과의 학생
var departNM = "컴퓨터공학과"
var departNM = "광고홍보학과"
//departNM에 담긴 학과의 학생들의 학번만 존재
var students_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO"))
students_in_departNM.show

//studentDF_test 중 한명의 학번
//세무회계학과의 학생
var std_NO = 20190030
var studentNO = students_in_departNM.filter(students_in_departNM("STD_NO").equalTo(s"${std_NO}"))

//--------------------from. 교과목수료 테이블 V_STD_CDP_SUBJECT : 학과이름, 학번, 수업명-------------------------
var clPassUri_DF = clPassUri_table.select(col("SUST_CD_NM"), col("STD_NO"), col("SBJT_KOR_NM")).distinct.toDF

//컴퓨터공학과에서 개설된 교과목 데이터프레임
var sbjtNM_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}"))
sbjtNM_in_departNM.show

//@@@ 컴퓨터공학과에서 개설된 수업명을 리스트로 생성
var sbjtNM_List = sbjtNM_in_departNM.select("SBJT_KOR_NM").rdd.map(r=>r(0)).collect.toList

//컴퓨터공학과 학생 학번을 가지고 그 사람이 수강한 교과목 리스트 생성
//학과로 filter
//학번으로 filter해서
//수업명을 리스트로 생성

var departNM = "컴퓨터공학과"
var std_NO = 20190030
var student_have_sbjt_temp1 = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}"))
var student_have_sbjt_temp2 = student_have_sbjt_temp1.filter(student_have_sbjt_temp1("STD_NO").equalTo(s"${std_NO}"))
var student_have_sbjt_temp3 = student_have_sbjt_temp2.select(col("SBJT_KOR_NM"))

//@@@ 컴퓨터공학과의 학생 한명이 수강한 수업 리스트를 생성
var student_have_sbjt_List = student_have_sbjt_temp3.select("SBJT_KOR_NM").rdd.map(r=>r(0)).collect.toList.distinct

val isListened_List_temp1 = sbjtNM_List.map(x => (x, 0)).map{ record =>
  //x : record_1
  //0 : record_2
  //isListend면 1로 바뀜
  val name = record._1
  val isListened =
    if(student_have_sbjt_List.contains(name)) {
      1
    }
    else 0
  val isListened_List_temp2 = (name, isListened)
  print(isListened_List_temp2)
  //리턴하려면 이름을 쳐야 함
  //최종적으로 isListened_List_temp1 = isListened_List_temp2 값이 담기는 것 !!
  isListened_List_temp2
}
val isListened_List = isListened_List_temp1.map(_._2)

//-------------------- # # # 자율활동 리스트 # # # ------------------------------
//from.교외활동 CPS_OUT_ACTIVITY_MNG : 학번(OAM_STD_NO), 활동구분코드(OAM_TYPE_CD), 활동명(OAM_TITLE)
//자격증(CD01) : 이름(OAM_TITLE) / ex. 토익800~900, FLEX 일본어 2A,  FLEX 일본어 1A,  FLEX 중국어 1A
//어학(CD02) : 이름(OAM_TITLE)
//봉사(CD03), 대외활동(CD04), 기관현장실습(CD05) : 활동구분코드(OAM_TYPE_CD)

//학과 학생들 서치
//학과df
//for문으로 거기 있는 학번을 입력하고 자율활동내역을 긁어오고 -> 학생리스트
//학과 리스트 생성
//횟수 리스트

//학번으로 학과 찾기
var showDepart_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${201937029}"))

import spark.implicits._

var outActUri_DF = outActUri_table.select(col("OAM_STD_NO"), col("OAM_TYPE_CD"), col("OAM_TITLE"))
var departNM = "광고홍보학과"
var std_NO1 = 201937039
var std_NO2 = 20130001
var clPassUri_DF = clPassUri_table.select(col("SUST_CD_NM"), col("STD_NO")).distinct.toDF
var students_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO"))
//map연산은 dataframe에 쓸 수 없기 때문에 list로 변환해야 하며 dataframe을 list로 변환하려면 df의 값 하나하나에 접근하기 위해 map 연산이 필요함

//광홍과df(clpass 교과목 수료 테이블에서 학과 별 학번 dataframe을 생성한 뒤 list로 변환)
var stdNO_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO")).rdd.map(r=>r(0)).collect.toList

//광홍과 학생 중 자율활동 데이터가 있는 학생은 극소수
// var stdNo_test_df =  outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${20132019}")).select(col("OAM_TYPE_CD"), col("OAM_TITLE"))

//광홍과 학번을 돌면서



var activity_List_byStd = List[Any]()

stdNO_in_departNM.foreach{ stdNO =>
  var depart_activity_temp = List[Any]()
  var depart_code_list = List[Any]("OAMTYPCD03", "OAMTYPCD04", "OAMTYPCD05")

  //List1 : 코드(중복제거x) -> map 함수로 df에서 list 변환
  //List2 : 이름(중복제거) -> map 함수로 df에서 list 변환
  //List3 : List1 + List2 = 코드리스트 + 이름리스트 (학생 한명이 수행한 자율활동내용)
  //---------------------자율활동 code list(자격증01, 어학02)----------------------
  //5개의 코드 (학생 한명이 수행한 봉사03, 대외04, 기관05을 코드 별로 groupby count list)
  var outAct_code_temp1 = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdNO}")).select(col("OAM_STD_NO"),col("OAM_TYPE_CD"), col("OAM_TITLE"))
  //3개의 코드만 필터링
  var outAct_code_temp2 = outAct_code_temp1.filter($"OAM_TYPE_CD" === "OAMTYPCD03" || $"OAM_TYPE_CD" ==="OAMTYPCD04" || $"OAM_TYPE_CD" ==="OAMTYPCD05")
  //학생 한명의 활동 코드만 존재하는 dataframe
  var outAct_code_temp3 = outAct_code_temp2.drop("OAM_STD_NO", "OAM_TITLE").groupBy("OAM_TYPE_CD").count()

  //모든 학과 모든 학생이 수행한 자율활동은 총 845개인데
  //광홍과 201937039학생이 수행한 활동만 260개이고 나머지 광홍과 학생들의 데이터는 존재하지 않음

  //이제 코드 3개에 대해 groupby와 agg 연산을 사용하여 코드 별로 count
  var outAct_code_List = outAct_code_temp3.select(col("count")).rdd.map(r=>r(0)).collect.toList
  //------------------------------------------------------------------------------

  //---------------------자율활동 name list(자격증01, 어학02)----------------------
  //5개의 코드
  var outAct_name_temp1 = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdNO}")).select(col("OAM_STD_NO"), col("OAM_TITLE")).distinct
  //3개의 코드만 필터링
  var outAct_name_temp2 = outAct_name_temp1.drop("OAM_STD_NO", "OAM_TYPE_CD").filter($"OAM_TYPE_CD" === "OAMTYPCD01" || $"OAM_TYPE_CD" ==="OAMTYPCD02").distinct

  var outAct_name_List = outAct_name_temp2.rdd.map(r=>r(0)).collect.toList
  println(outAct_name_List)
  // var t_size = outAct_name_List.length
  // if(t_size > 0) {
    // println(s"##### SIZE : ${t_size} RESULT => " + outAct_name_List)
  // }

  depart_activity_temp = depart_activity_temp ++ outAct_name_List

  //----------codeList + nameList = 학생 하나의 리스트-----------------------------
  // var outAct_std_List = outAct_code_List ::: outAct_name_List
  // outAct_std_List
  //------------------------------------------------------------------------------

  //학과 리스트
  var depart_activity_List = depart_activity_temp.distinct
  println(depart_activity_List)

  //namelist로 유무 비교
  //학생 name list 랑 학과 list를 비교해서 contain으로 1, 0

  var activity_List_byStd_temp1 = depart_activity_List.map(x => (x, 0)).map{ activity =>
    //x : record_1
    //0 : record_2
    //isListend면 1로 바뀜
    val actName = activity._1
    val act =
      if(outAct_name_List.contains(actName)) {
        1
      }
      else 0
    var activity_List_byStd_temp2 = (actName, act)
    print(activity_List_byStd_temp2)
    //리턴하려면 이름을 쳐야 함
    //최종적으로 isListened_List_temp1 = isListened_List_temp2 값이 담기는 것 !!
    activity_List_byStd_temp2
  }
  var activity_List_byStd_temp3 = activity_List_byStd_temp1.map(_._2)
  println("activity_List_byStd:" + activity_List_byStd_temp3)
  //학과 리스트를 돌면서 일치 여부 세는데
  //봉사03, 대외04, 기관05 = 횟수 count
  //자격증01, 어학02 = 유무(1 또는 0)

 activity_List_byStd = outAct_code_List ++ activity_List_byStd_temp3
 //학생 별 코드 횟수, 이름 유무 리스트 출력
 println("activity List !!! : " + activity_List_byStd)
}
//------------------------------------------------------------------------------



//--------------------mongodb에서 collection 복제---------------------
// db.CPS_NCR_PROGRAM_INFO.find().forEach( function(x){db.CPS_NCR_PROGRAM_INFO2.insert(x)} )
//--------------------mongodb에서 데이터 삭제 : 지우기 전에 원본 데이터 복사해두고 하나만 insert------------
//db.CPS_NCR_PROGRAM_INFO2.remove({NPI_KEY_ID:"NCR000000000718"})





//-------------------- # # # 비교과 리스트 # # # --------------------------------
// from. 교과/비교과용 별점 테이블(CPS_STAR_POINT) : 학번(STD_NO), 비교과id(STAR_KEY_ID), 별점(STAR_POINT)
// from. 비교과 관련 테이블(CPS_NCR_PROGRAM_INFO) : 비교과id(NPI_KEY_ID), 중분류(NPI_AREA_SUB_CD)

//비교과 아이디(학번, 비교과id 사용 from.교과/비교과별점테이블)로 중분류 가져오기(비교과id, 중분류 from.비교과 관련 테이블)

//학과 별 학생들이 수강한 비교과의 중분류 list 로 포맷 잡고 : 학과 - 학번 돌면서 list 만들고 , 중분류로 바꿔주기
//학생 한명이 수강한 비교과 list -> 별점 가져오기(from. 교과/비교과 별점 테이블) -> 중분류 가져오기 -> 중분류 별 별점 avg 계산




// ---------------------------------------------------------------------------
// from. 비교과 테이블 : NCR~ ID를 이용해 별점 테이블에 데이터를 생성해줌 !!!!***
// var test = ncrInfoUri_DF.select(col("NPI_KEY_ID"),col("NPI_AREA_SUB_CD")).filter(ncrInfoUri_DF("NPI_KEY_ID").equalTo("NCR000000000718"))
var ncrInfoUri_DF = ncrInfoUri_table.select(col("NPI_KEY_ID"), col("NPI_AREA_SUB_CD"))
var get_NPI_KEY_ID = ncrInfoUri_DF.select(col("NPI_KEY_ID"),col("NPI_AREA_SUB_CD"))
//<학생 DataFrame> / 하나의 학과의 학생 => 지금 컴퓨터공학과 학생을 조회했으니까 컴퓨터공학과 학번을 조회해서 별점 테이블에 데이터를 삽입해줌
var departNM = "컴퓨터공학과"
//from. 교과 수료 테이블 : departNM에 담긴 학과의 학생들의 학번을 가져옴
var students_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO"))
students_in_departNM.show
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// 컴퓨터 공학과 학생 4명에 대해 별점 테이블 데이터 추가함 : |20142820||20142932| |20152611| |20152615|
// 1. 질의를 내린 학생의 학과를 검색
//

// ---------------------------------------------------------------------------




// from. 교과/비교과용 별점 테이블(CPS_STAR_POINT) : 학번(STD_NO), 비교과id(STAR_KEY_ID), 별점(STAR_POINT), 타입(TYPE)
var cpsStarUri_DF = cpsStarUri_table.select(col("STD_NO"), col("STAR_KEY_ID"), col("STAR_POINT"), col("TYPE"))
// from. 비교과 관련 테이블(CPS_NCR_PROGRAM_INFO) : 비교과id(NPI_KEY_ID), 중분류(NPI_AREA_SUB_CD)
var ncrInfoUri_DF = ncrInfoUri_table.select(col("NPI_KEY_ID"), col("NPI_AREA_SUB_CD"))

// 비교과 별점 => 별점 테이블에서 "TYPE"이 "N" :: ex) NCR000000000677
var cpsStarUri_ncr_DF = cpsStarUri_DF.filter(cpsStarUri_DF("TYPE").equalTo("N"))
// 교과 별점 => 별점 테이블에서 "TYPE"이 "C" :: ex) BAQ00028
var cpsStarUri_sbjt_DF = cpsStarUri_DF.filter(cpsStarUri_DF("TYPE").equalTo("C"))

var departNM = "컴퓨터공학과"
var std_NO1 = 20190030
var std_NO2 = 20142820
var std_NO3 = 201403065


// from. 교과목수료 테이블 : 학과명, 학번 => 질의를 내린 학생의 학과를 찾기 위해 사용
var clPassUri_DF = clPassUri_table.select(col("SUST_CD_NM"), col("STD_NO")).distinct.toDF

//학번으로 학생의 학과 찾기 (dataframe) => 컴퓨터공학과 학생
var showDepart_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO2}"))

//학번으로 별점 가져오기 (dataframe) => 학생 한 명에 대한 별점
var getStar_by_stdNO = cpsStarUri_DF.filter(cpsStarUri_DF("STD_NO").equalTo(s"${std_NO2}"))

// from.별점테이블 (cpsStarUri_DF) => 학번에 따라 프로그램 code 가져오기 + 내용을 list에 저장
// ncrInfoUri_DF.filter(ncrInfoUri_DF("NPI_KEY_ID").equalTo(s"${}"))

// 학생 한 명에 대해서 별점테이블을 조회해서 교과/비교과 관련 활동 KEY_ID를 가져옴 => List 생성
var key_id_temp = cpsStarUri_ncr_DF.select(col("STAR_KEY_ID")).filter(cpsStarUri_DF("STD_NO").equalTo(s"${std_NO2}"))
var key_id_List = key_id_temp.rdd.map(r=>r(0)).collect.toList



var sub_cd_List = List[Any]()
var sub_star_List = List[Any]()
var ncr_List = List[Any]()
var ncr_List_temp = List[Any]()
var emptyDF = spark.emptyDataset[KV].toDF

import org.apache.spark.sql.types.{
    StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.Row

val schema1 = StructType(
    StructField("STAR_POINT", StringType, true) ::
    StructField("STAR_KEY_ID", StringType, true) :: Nil)
var star_keyid_DF = spark.createDataFrame(sc.emptyRDD[Row], schema1)


val schema2 = StructType(
    StructField("NPI_AREA_SUB_CD", StringType, true) ::
    StructField("NPI_KEY_ID", StringType, true) :: Nil)
var subcd_keyid_DF = spark.createDataFrame(sc.emptyRDD[Row], schema2)


val schema3 = StructType(
    StructField("STAR_POINT", StringType, true) ::
    StructField("NPI_KEY_ID", StringType, true) :: Nil)
var star_subcd_DF = spark.createDataFrame(sc.emptyRDD[Row], schema3)



// 비교과 활동 KEY_ID를 비교과 테이블에서 찾음 => 그 KEY_ID를 검색해 중분류를 가져옴


// 비교과활동 KEY_ID 하나에 따른 별점
// var star_point_list_temp = getStar_by_stdNO.select(col("STAR_POINT")).filter(getStar_by_stdNO("STAR_KEY_ID").equalTo("NCR000000000694"))

// var tmp = List[Any]()
// 학생 한 명이 수행한 비교과 프로그램 keyid
key_id_List.foreach{ keyid =>

  //비교과 id 로 중분류 가져오기(비교과id, 중분류 from.비교과 관련 테이블) (dataframe)
  var sub_cd_List_temp = ncrInfoUri_DF.select(col("NPI_AREA_SUB_CD"),col("NPI_KEY_ID")).filter(ncrInfoUri_DF("NPI_KEY_ID").equalTo(s"${keyid}"))
  // sub_cd_List_temp.show

  // 중분류 dataframe을 list로 변환
  // var sub_cd_List_temp2 = sub_cd_List_temp.rdd.map(r=>r(0)).collect.toList
  //
  // ncr_List_temp = sub_cd_List_temp.collect.toList.map(_.toSeq)
  //
  // ncr_List = ncr_List ++ sub_cd_List_temp.collect.toList.map(_.toSeq)

  // ncr_List = ncr_List ++ sub_cd_List_temp2

  // 중분류 code list 생성
  // sub_cd_List = sub_cd_List ++ sub_cd_List_temp2

  // 비교과 활동에 대한 별점을 가져옴
  var star_point_List_temp = getStar_by_stdNO.select(col("STAR_POINT"),col("STAR_KEY_ID")).filter(getStar_by_stdNO("STAR_KEY_ID").equalTo(s"${keyid}"))
  // star_point_List_temp.show
  // var star_point_List_temp2 = star_point_List_temp.rdd.map(r=>r(0)).collect.toList

  star_keyid_DF = star_keyid_DF.union(star_point_List_temp)

  subcd_keyid_DF = subcd_keyid_DF.union(sub_cd_List_temp)
}

// df.dropDuplicates("timestamp")

star_keyid_DF = star_keyid_DF.dropDuplicates("STAR_KEY_ID")
subcd_keyid_DF = subcd_keyid_DF.dropDuplicates("NPI_KEY_ID")

star_keyid_DF.show
subcd_keyid_DF.show

//학과 별 학생들이 수강한 비교과의 중분류 list 로 포맷 잡고 : 학과 - 학번 돌면서 list 만들고 , 중분류로 바꿔주기
//학생 한명이 수강한 비교과 list -> 별점 가져오기(from. 교과/비교과 별점 테이블) -> 중분류 가져오기 -> 중분류 별 별점 avg 계산


// 재용오빠가 mongodb에서 중복데이터 제거하는 코드짠 뒤 다시 테스트
