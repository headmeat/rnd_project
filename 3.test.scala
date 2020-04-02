//var(varue) : 변경불가
//var(variable) : 변경가능
//:paste로 해서 넣어야 eof 에러 안남 !!
//~.select 의 반환값이 spark.sql.row ~


//<학과 DataFrame> : departDF / 전체 학과의 모든 학생
//from. 학생 정보 테이블 : 학과명, 학번
var departDF = stInfoUri_table.select(col("SUST_CD_NM"), col("STD_NO")).distinct.toDF


//<학생 DataFrame> / 하나의 학과의 학생
//dataframe의 값을 filter로 한개만 가져오기

// var studentDF = departDF.filter(departDF("SUST_CD_NM").equalTo("세무회계학과")).select(col("STD_NO"))


//나중에 사용자가 입력한 과, 학번으로 변수 값이 변경되며
//일단 모든 학과의 모든 학생이 반복문으로 들어가서 고유 리스트를 생성해야 함!!
var departNM = "세무회계학과"
//departNM에 담긴 학과의 학생들의 학번
var studentDF_test = departDF.filter(departDF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO"))




//학과의 한명의 학생
//학과 데이터프레임에서 학번으로 필터링해서 가져오면 되지!!
// var studentNo_test = studentDF.filter(studentDF("STD_NO").equalTo("201928011"))

var std_NO = 201928011
var studentNO = studentDF.filter(studentDF("STD_NO").equalTo(s"${std_no}"))



//1) 모든 학과, 모든 학생의 교과정보 리스트
//하나의 과, 하나의 학생의 교과정보 리스트를 먼저 생성해보자. -> 그 이후에 반복문 돌면서 모든 학생에 대한 리스트 생성
//학번을 가지고 교과목수료 테이블에서 수강 여부를 가져와야 함.
//교과목애 대한 리스트 먼저 생성 !!
//수강 여부는 그 다음..

//학과에 개설 된 교과목 정보 모두 출력하기
//from. 교과정보 테이블 V_STD_CDP_SUBJECT



var classInfoDF = clInfoUri_table.select(col("SBJT_KEY_CD"), col("SUST_CD_NM"), col("SBJT_KOR_NM")).distinct.toDF

// var classInfoDF_test = classInfoDF.filter(classInfoDF("SUST_CD_NM").equalTo("컴퓨터공학과"))
// var classInfoDF_test2 = classInfoDF.filter(classInfoDF("SUST_CD_NM").equalTo(s"${departNM}"))
//이 학과 별 수업 정보를 리스트로 만들어야 함


//var classInfoDF_test = classInfoDF.filter(classInfoDF("SBJT_KEY_CD").equalTo("컴퓨터공학과"))
var classInfoDF_test2 = classInfoDF.filter(classInfoDF("SUST_CD_NM").equalTo(s"${departNM}"))

// var classNM_test = classInfoDF_test2.select(col("SBJT_KOR_NM"))
// var classInfoList = classNM_test.select("SBJT_KEY_CD").rdd.map(r=>r(0)).collect.toList

var classKey_test = classInfoDF_test2.select(col("SBJT_KOR_NM"))
var classInfoList = classKey_test.select("SBJT_KOR_NM").rdd.map(r=>r(0)).collect.toList











// //from 교과정보 테이블 !!
// //학과코드 리스트를 생성 !
// var classInfoDF = clInfoUri_table.select(col("SUST_CD"),col("SUST_CD_NM"),col("SBJT_KEY_CD"),col("SBJT_KOR_NM")).distinct.toDF
// //호텔관광경영
// //숫자가 0부터 넣으면 생성 불가 -> string으로 감싸줘야 함 ㅡㅡ
var departKey = "00065"
// var sbjt_key_from_depart_List = classInfoDF.select("SBJT_KEY_CD").rdd.map(r=>r(0)).collect.toList.distinct
//
//




//교과목 수료 테이블에서 학번으로 수업list 중 하나를 들었는지 여부 리턴
//from. 교과목 수료 테이블
var clpass = clPassUri_table.select(col("SUST_CD"), col("SBJT_KOR_NM"), col("STD_NO"))

//학과로 정제, 학번으로 질의를 내려서 수업코드만 가져오기

var getSBJT_key_temp1 = clpass.filter(clpass("SUST_CD").equalTo(s"${departKey}"))
var getSBJT_key_temp2 = getSBJT_key_temp1.filter(getSBJT_key_temp1("STD_NO").equalTo(s"${std_NO}"))
var getSBJT_key_temp3 = getSBJT_key_temp2.select(col("SBJT_KOR_NM"))

var getSBJT_key_from_depart_List = getSBJT_key_temp3.select("SBJT_KOR_NM").rdd.map(r=>r(0)).collect.toList.distinct

var classInfoList_print = classInfoList.foreach {println}
var getSBJT_key_from_depart_print = getSBJT_key_from_depart_List.foreach {println}







// classInfoList.foreach{name =>
//   if(getSBJT_key_from_depart_List.contains(name)){
//     print("yes")
//   }
// }
//
// val m1 = List(1, 3, 5, 2)
// val res = m1.contains(1)
// println(res)
//
// var test = 0
//
// if(res == true){
//    test = 1
// }
//
// println(test)



var test = 0
classInfoList.foreach{name =>
  print(name)
  // val res = getSBJT_key_from_depart_List.contains(name)
  //
  // if(res == true){
  //    test = 1
  // }
  // print(test)
}

// getSBJT_key_from_depart_List.foreach{name1 =>
//   print(name1)
// }







var studentArr = Array
var dept = Array

departArr.foreach{ dep =>
  var studentArr_temp1 = stInfoUri_table.select(col("SUST_CD"), col("SUST_CD_NM"), col("STD_NO"))
  //studentArr_temp1
  var dep_toString = dep.toString
  //dept는 Array
  dept = dep_toString.substring(1, dep_toString.size-1)
  //println(s"dept : ${dept}")

  //var studentArr_temp2 = studentArr_temp1.select($"SUST_CD_NM", $"STD_NO").where($"SUST_CD_NM"===s"$dept")
  //var studentArr_temp2 = studentArr_temp1.select($"SUST_CD_NM", $"STD_NO").where($"SUST_CD_NM"===dept(1))

  //univ1.show
  //studentArr_temp2.show

 //  studentArr = studentArr_temp2.select($"STD_NO").collect
 //  println(s"studentArr : ${studentArr}")
 // //
 // var outAct_temp1 = outActUri_table.select(col("OAM_STD_NO"), col("OAM_TYPE_CD"))
 // //outAct_temp1.show
 // var outAct_student_temp = studentArr(1)
 // var outAct_temp2 = outAct_temp1.where($"OAM_STD_NO"===s"$outAct_student_temp")
 // outAct_temp2
//  println(outAct_temp2)
// println(s"outAct_temp2 : ${outAct_temp2}")
}
println(studentArr.mkString(","))
println(studentArr(1))
