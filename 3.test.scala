//-------------------- # # # 교과목 리스트 # # # --------------------------------
//--------------------from. 교과목수료 테이블 : 학과명, 학번----------------------
//<학과 DataFrame> : departDF / 전체 학과의 모든 학생
//###학과 별 학생 번호 보기 ###
var clPassUri_DF = clPassUri_table.select(col("SUST_CD_NM"), col("STD_NO")).distinct.toDF
clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo("컴퓨터공학과")).show


//@@@ 세무회계학과에서 개설된 수업이 없는 경우는 교과목리스트를 생성할 수 없음 => 교과목을 제외하고 유사도 비교를 수행해야 함 @@@
clPassUri_table.select(col("SUST_CD_NM"), col("SBJT_KOR_NM")).distinct.toDF.filter(clPassUri_table("SUST_CD_NM").equalTo("컴퓨터공학과")).show

//<학생 DataFrame> / 하나의 학과의 학생
var departNM = "컴퓨터공학과"
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


-------------------- # # # 자율활동 리스트 # # # ------------------------------
from.교외활동 CPS_OUT_ACTIVITY_MNG : 학번(OAM_STD_NO),          활동명(OAM_TITLE)
자격증(CD01) : 이름(OAM_TITLE) / ex. 토익800~900, FLEX 일본어 2A,  FLEX 일본어 1A,  FLEX 중국어 1A
어학(CD02) : 이름(OAM_TITLE)
봉사(CD03), 대외활동(CD04), 기관현장실습(CD05) : 활동구분코드(OAM_TYPE_CD)


var outActUri_DF = outActUri_table.select(col("OAM_STD_NO"), col("OAM_TYPE_CD"), col("OAM_TITLE"))
var outActUri_DF = outActUri_table.select(col("OAM_STD_NO"), col("OAM_TYPE_CD"), col("OAM_TITLE")).distinct.toDF
outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo("201937039")).show

var OAM_TITLE_test =
var OAM_TYPE_CD_test = "분류코드"
var OAM_STD_NO = 201937039`

var outActUri_DF = outActUri_table.select(col("OAM_STD_NO"), col("OAM_TYPE_CD"), col("OAM_TITLE")).distinct.toDF

var students_active_departNM = outActUri_DF.filter(outActUri_DF("OAM_TITLE").equalTo(s"${}"))
students_in_departNM.show
var students_active_list = students_active_departNM.select("OAM_TITLE").rdd.map(r=>r(0)).collect.toList

//@@@ 세무회계학과에서 개설된 수업이 없는 경우는 교과목리스트를 생성할 수 없음 => 교과목을 제외하고 유사도 비교를 수행해야 함 @@@
outActUri_table.select(col("OAM_STD_NO"), col("OAM_TYPE_CD"), col("OAM_TITLE")).distinct.toDF.filter(outActUri_table("OAM_STD_NO").equalTo("201937039")).show
outActUri_table.select(col("OAM_STD_NO"), col("OAM_TYPE_CD"), col("OAM_TITLE")).distinct.toDF.filter(outActUri_table("OAM_TYPE_CD").equalTo("OAMTYPCD01")).show

//<학생 DataFrame> / 하나의 학과의 학생
var departNM = "컴퓨터공학과"
//departNM에 담긴 학과의 학생들의 학번만 존재
var students_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO"))
students_in_departNM.show

//studentDF_test 중 한명의 학번
//세무회계학과의 학
var std_NO = 201937039//자율활동 테이블 임의 학번
var studentNO = students_in_departNM.filter(students_in_departNM("STD_NO").equalTo(s"${std_NO}"))
