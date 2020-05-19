spark-shell --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.1

/home/opc/blockvolume/apps/spark/bin/spark-shell --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.1

// /spark-shell --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.1


stdNO_in_departNM_List.flatMap{stdNO =>

  val star_temp_bystdNO_DF = cpsStarUri_sbjt_DF.filter(cpsStarUri_sbjt_DF("STD_NO").equalTo(s"${stdNO}"))

  val res =
    star_temp_bystdNO_DF
    .collect // dataframe 를 collect 해서 array[row] 를 받음
    .map(record => (record(0).toString, starPoint(record(1).toString, record(2).toString)))
    // 각 row에 대해서 (학번, starPoint 객체) 로 바꿈
    .groupBy(x => x._1) // 이걸 하면 데이터 타입이 맵(학번, )
    .map( x => (x._1, x._2.map(x => x._2)))
    // 그룹바이를 학번으로 해서 (학번, Array)
  res

}



var sbjtCD_star_byStd_Map = stdNO_in_departNM_List.flatMap{ stdNO =>
  // 학번 별 학번-교과코드-별점
  val star_temp_bystdNO_DF = cpsStarUri_sbjt_DF.filter(cpsStarUri_sbjt_DF("STD_NO").equalTo(s"${stdNO}"))

  val res =
    star_temp_bystdNO_DF
    .collect // dataframe 를 collect 해서 array[row] 를 받음
    .map(record => (record(0).toString, starPoint(record(1).toString, record(2).toString)))
    // 각 row에 대해서 (학번, starPoint 객체) 로 바꿈
    .groupBy(x => x._1) // 이걸 하면 데이터 타입이 맵(학번, )
    .map( x => (x._1, x._2.map(x => x._2)))
    // 그룹바이를 학번으로 해서 (학번, Array)
  res
}.toMap





sqlDF.foreach { row =>
         row.toSeq.foreach{col => println(col) }
  }



val df = Seq(cls_Employee("Andy","aaa", 20), cls_Employee("Berta","bbb", 30), cls_Employee("Joe","ccc", 40)).toDF()
df.as[cls_Employee].take(df.count.toInt)\
.foreach(t => println(s"name=${t.name},sector=${t.sector},age=${t.age}"))


for (row <- df.rdd.collect)
{
  var name = row.mkString(",").split(",")(0)
  var sector = row.mkString(",").split(",")(1)
  var age = row.mkString(",").split(",")(2)
}
