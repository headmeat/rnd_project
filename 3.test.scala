//val(value) : 변경불가
//var(variable) : 변경가능
//:paste로 해서 넣어야 eof 에러 안남 !!
//~.select 의 반환값이 spark.sql.row ~

//<학과배열 > : departArr
//<학과 DataFrame> : departDF

var departDF = stInfoUri_table.select(col("SUST_CD_NM"), col("STD_NO")).distinct.toDF
// val departArr = departArr_temp2.collect.distinct
// print(departArr.mkString(","))




//<학생 DataFrame>


//dataframe의 값을 filter로 한개만 가져오기

var studentDF_temp = departDF.filter(departDF("SUST_CD_NM").equalTo("세무회계학과"))

var studentDF = studentDF_temp.select(col("STD_NO"))




var studentArr = Array
var dept = Array

departArr.foreach{ dep =>
  val studentArr_temp1 = stInfoUri_table.select(col("SUST_CD"), col("SUST_CD_NM"), col("STD_NO"))
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
 // val outAct_temp1 = outActUri_table.select(col("OAM_STD_NO"), col("OAM_TYPE_CD"))
 // //outAct_temp1.show
 // var outAct_student_temp = studentArr(1)
 // var outAct_temp2 = outAct_temp1.where($"OAM_STD_NO"===s"$outAct_student_temp")
 // outAct_temp2
//  println(outAct_temp2)
// println(s"outAct_temp2 : ${outAct_temp2}")
}
println(studentArr.mkString(","))
println(studentArr(1))










  //a학과의 b학생이 수행한 자율활동 정보 가져오기
// dep == "tempdept컴공"
// 학생 == "학번"
// 자율활동테이블에서 수행한 값 가져오기
// 학번을 쳐서 활동코드 가져오기

//학과-학번을 가져와서
//자율활동테이블에서 학번으로 활동정보 가져오기



//학과 == 컴공
//학생 한명
// 한명에 대한 자율활동 정보
