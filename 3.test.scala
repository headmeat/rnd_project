//--------------------from. 교과목수료 테이블 : 학과명, 학번-------------------------
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



// //--------------------from. 교과정보 테이블 V_STD_CDP_SUBJECT : 학과이름, 교과목코드, 수업명-------------------------
//
// var clInfoUri_DF = clInfoUri_table.select(col("SUST_CD_NM"), col("SBJT_KEY_CD"), col("SBJT_KOR_NM")).distinct.toDF
//
// //세무회계학과에서 개설된 교과목 데이터프레임
// var sbjtNM_in_departNM = clInfoUri_DF.filter(clInfoUri_DF("SUST_CD_NM").equalTo(s"${departNM}"))
// sbjtNM_in_departNM.show
// // var classNM_test = classInfoDF_test2.select(col("SBJT_KOR_NM"))
// // var classInfoList = classNM_test.select("SBJT_KEY_CD").rdd.map(r=>r(0)).collect.toList
//
// var classKey_test = classInfoDF_test2.select(col("SBJT_KOR_NM"))
// var classInfoList = classKey_test.select("SBJT_KOR_NM").rdd.map(r=>r(0)).collect.toList




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







sbjtNM_List.foreach{name =>
  //print(name)
  val res = student_have_sbjt_List.contains(name)
  // print(res)
  if(res == true){
     test = 1
  }
  print(test)
}





val name
student_have_sbjt_List.foreach{x =>
  // println(x)
  // var name = "test"
  val name = x
  println(x)
  // val res = student_have_sbjt_List.contains(name)
  // // print(res)
  // if(res == true){
  //    test = 1
  // }
  // print(test)
}









var test = 0
sbjtNM_List.foreach{name =>
  //print(name)
  val res : Boolean = student_have_sbjt_List.exists(name => name == "")
  // print(res)
  if(res == true){
     test = 1
  }
  print(test)
}


val doesPlainDonutExists: Boolean = donuts.exists(donutName => donutName == "Plain Donut")

// var classInfoList_print = classInfoList.foreach {println}
// var getSBJT_key_from_depart_print = getSBJT_key_from_depart_List.foreach {println}



for( a <- 1 to 3; b <- 10 to 12){
   println(a,b)
}







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





// getSBJT_key_from_depart_List.foreach{name1 =>
//   print(name1)
// }
