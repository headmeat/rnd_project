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
var sbjtNM_List = sbjtNM_in_departNM.drop("SUST_CD_NM").select("SBJT_KOR_NM").rdd.map(r=>r(0)).collect.toList



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
var std_NO = 201937039

var clPassUri_DF = clPassUri_table.select(col("SUST_CD_NM"), col("STD_NO")).distinct.toDF
var students_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO"))
//map연산은 dataframe에 쓸 수 없기 때문에 list로 변환해야 하며 dataframe을 list로 변환하려면 df의 값 하나하나에 접근하기 위해 map 연산이 필요함

//광홍과df(clpass 교과목 수료 테이블에서 학과 별 학번 dataframe을 생성한 뒤 list로 변환)
var stdNO_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO")).rdd.map(r=>r(0)).collect.toList

//광홍과 학생 중 자율활동 데이터가 있는 학생은 극소수
// var stdNo_test_df =  outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${20132019}")).select(col("OAM_TYPE_CD"), col("OAM_TITLE"))

//광홍과 학번을 돌면서


// stdNO_in_departNM.foreach{stdNO =>
var depart_activity_List = List[Any]()

stdNO_in_departNM.slice(0, 10).foreach{ stdNO =>
  //List1 : 코드(중복제거x) -> map 함수로 df에서 list 변환
  //List2 : 이름(중복제거) -> map 함수로 df에서 list 변환
  //List3 : List1 + List2 = 코드리스트 + 이름리스트 (학생 한명이 수행한 자율활동내용)
  //---------------------자율활동 code list(자격증01, 어학02)----------------------
  //5개의 코드

  var tempNum = stdNO
  var outAct_code_temp1 = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${201937039}")).select(col("OAM_STD_NO"),col("OAM_TYPE_CD"), col("OAM_TITLE"))
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
  var outAct_name_temp1 = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${201937039}")).select(col("OAM_STD_NO"), col("OAM_TITLE")).distinct
  //3개의 코드만 필터링
  var outAct_name_temp2 = outAct_name_temp1.drop("OAM_STD_NO", "OAM_TYPE_CD").filter($"OAM_TYPE_CD" === "OAMTYPCD01" || $"OAM_TYPE_CD" ==="OAMTYPCD02").distinct

  var outAct_name_List = outAct_name_temp2.rdd.map(r=>r(0)).collect.toList

  var t_size = outAct_name_List.length
  if(t_size > 0) {
    //println(s"##### SIZE : ${t_size} RESULT => " + outAct_name_List)
  }

  depart_activity_List = depart_activity_List ++ outAct_name_List

  //----------codeList + nameList = 학생 하나의 리스트-----------------------------
  // var outAct_std_List = outAct_code_List ::: outAct_name_List
  // outAct_std_List
  //------------------------------------------------------------------------------

  //학과 리스트
  // var outAct_depart_temp1 = List("OAMTYPCD01", "OAMTYPCD02", "OAMTYPCD03")
  //
  // //name list를 합치고 중복 제거
  //
  // //모든 학생의 리스트를 for문으로 만들어가면서 append 시키고 중복 제거
  // var outAct_depart_temp2 =
}
depart_activity_List










// val student_activity_list_temp1 = students_in_departNM.map{ stdNO =>
//   // record == 학번
//   // 학생이 수행한 자율활동 리스트 : 봉사코드, 대외코드, 기관코드, / 자격증 이름, 어학 이름
//   var test1 = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdNO}")).select(col("OAM_TYPE_CD"), col("OAM_TITLE"))
//   test1
// }
