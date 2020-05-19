spark-shell --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.1

/home/opc/blockvolume/apps/spark/bin/spark-shell --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.1

// /spark-shell --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.1
aps.collect().foreach(row => <do something>)


val dftest = Seq(
 ("20130525"), ("20160403"), ("20170773")
).toDF("stdNO")


dftest.foreach{ stdNo =>
  var star_point_List = List[Any]() // 학번당 별점을 저장
  var orderedIdx_byStd = List[Int]() //학번 당 교과 리스트
  var not_orderedIdx_byStd = List[Int]()

  for(i<-0 until sbjtCD_in_departNM_List.size){
      star_point_List = -1.0::star_point_List
  }

  var sbjtNM_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${stdNo}")).select(col("SBJT_KEY_CD"))
  var sbjtNM_by_stdNO_List = sbjtNM_by_stdNO.rdd.map(r=>r(0)).collect.toList.distinct

  var getStar_by_stdNO = cpsStarUri_sbjt_DF.filter(cpsStarUri_sbjt_DF("STD_NO").equalTo(s"${stdNo}")).select("STAR_KEY_ID").collect.map({row=>
     val str = row.toString
     val size = str.length
     val res = str.substring(1, size-1).split(",")
     val list_ = res(0)
     list_})

  var not_rated = sbjtNM_by_stdNO_List filterNot getStar_by_stdNO.contains
  // println(not_rated)
  // stdNo_List_byMap_sbjt

  if(stdNo_List_byMap_sbjt.contains(s"${stdNo}")){
    var valueBystdNo_from_Map = sbjtCD_star_byStd_Map(s"${stdNo}") //!
    for(i<-0 until valueBystdNo_from_Map.size){
      //학생 한명의 중분류-별점 맵에서 중분류 키에 접근 : valueBystdNo_from_Map(i).subcd)
      //학생 한명이 들은 중분류 리스트를 가져옴
      orderedIdx_byStd = sbjtCD_in_departNM_List.indexOf(valueBystdNo_from_Map(i).sbjtCD)::orderedIdx_byStd
      // println("orderedIdx_byStd ===> " + orderedIdx_byStd)
    }

    for(i<-0 until not_rated.size){
      not_orderedIdx_byStd = sbjtCD_in_departNM_List.indexOf(not_rated(i).toString)::not_orderedIdx_byStd
      // println("not_orderedIdx_byStd ===> " + not_orderedIdx_byStd)
    }

    orderedIdx_byStd = orderedIdx_byStd.sorted
    // println("orderedIdx_byStdsorted ===>" + orderedIdx_byStd)

    not_orderedIdx_byStd = not_orderedIdx_byStd.sorted
    // println("not_orderedIdx_byStd ===>" + not_orderedIdx_byStd)

    for(i<-0 until not_orderedIdx_byStd.size){
      star_point_List = star_point_List.updated(not_orderedIdx_byStd(i), -1)
    }

    for(i<-0 until orderedIdx_byStd.size){ // orderedIdx_byStd 크기 (학번당 들은 중분류를 for문 돌림)
      var k=0;
      //print(k)
      // 학과 전체의 중분류 리스트와 학생의 중분류 리스트의 값이 같을때까지 k를 증가
      while(sbjtCD_in_departNM_List(orderedIdx_byStd(i))!= valueBystdNo_from_Map(k).sbjtCD){
      k+=1;
      }
      // 같은 값이 나오면 0으로 설정돼있던 값을 (그 자리의 값을) 학생의 별점으로 바꿔줌
      star_point_List = star_point_List.updated(orderedIdx_byStd(i), valueBystdNo_from_Map(k).starpoint)
      // println(s"$star_point_List")
    }
  }
  val star_list = star_point_List.map(x => x.toString.toDouble)
  // println(">>"+star_list)
  sbjt_tuples = sbjt_tuples :+ (stdNo.toString, star_list)
}


dftest.map{stdNO =>
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

stdNO_in_departNM_sbjt.flatMap {
  case (x) => x.split(",").map((x))
}.toDF

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



20130525, 20160403, 20170773, 20162617, 20191326, 20142804, 20162612, 20190201, 20180495, 20142810, 20152643, 20190842, AM20191014, 20152611, 20190765, 20142813, 20180360, 20191225, 20180700, 201906028, 20190395, 20130533, 20190907, 20130518, 20162642, 20152647, 20152651, 20190109, 20162625, 201902046, 20190704, 20190002, 20142910, 20142808, 20130512, 20190287, 20142926, 201906023, 20120635, 20191207, 20180601, 20190030, 20180890, 20180748, 20191039, 20171424, 20152625, 20142820, 20130521, 20152630, 20171441, 20171415, 20171429, 20181042, 20190809, 20142822, 20190138, 20191337, 20190602, 20190447, 20120510, 20190936, 20171227, 20180623, 20130531, 20180835, 201906047, 20130632, 201901021, 201906026, 20162604, 20130516, 20142832, 20142829,





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












//=========================================================================

var df1 = stdNo_List_byMap_sbjt.toDF
df1.foreach{ stdNo =>
  var star_point_List = List[Any]() // 학번당 별점을 저장
  var orderedIdx_byStd = List[Int]() //학번 당 교과 리스트
  var not_orderedIdx_byStd = List[Int]()

  for(i<-0 until sbjtCD_in_departNM_List.size){
      star_point_List = -1.0::star_point_List
  }

  var sbjtNM_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${stdNo}")).select(col("SBJT_KEY_CD"))
  var sbjtNM_by_stdNO_List = sbjtNM_by_stdNO.rdd.map(r=>r(0)).collect.toList.distinct

  var getStar_by_stdNO = cpsStarUri_sbjt_DF.filter(cpsStarUri_sbjt_DF("STD_NO").equalTo(s"${stdNo}")).select("STAR_KEY_ID").collect.map({row=>
     val str = row.toString
     val size = str.length
     val res = str.substring(1, size-1).split(",")
     val list_ = res(0)
     list_})

  var not_rated = sbjtNM_by_stdNO_List filterNot getStar_by_stdNO.contains
  // println(not_rated)
  // stdNo_List_byMap_sbjt

  if(stdNo_List_byMap_sbjt.contains(s"${stdNo}")){
    var valueBystdNo_from_Map = sbjtCD_star_byStd_Map(s"${stdNo}") //!
    for(i<-0 until valueBystdNo_from_Map.size){
      //학생 한명의 중분류-별점 맵에서 중분류 키에 접근 : valueBystdNo_from_Map(i).subcd)
      //학생 한명이 들은 중분류 리스트를 가져옴
      orderedIdx_byStd = sbjtCD_in_departNM_List.indexOf(valueBystdNo_from_Map(i).sbjtCD)::orderedIdx_byStd
      // println("orderedIdx_byStd ===> " + orderedIdx_byStd)
    }

    for(i<-0 until not_rated.size){
      not_orderedIdx_byStd = sbjtCD_in_departNM_List.indexOf(not_rated(i).toString)::not_orderedIdx_byStd
      // println("not_orderedIdx_byStd ===> " + not_orderedIdx_byStd)
    }

    orderedIdx_byStd = orderedIdx_byStd.sorted
    // println("orderedIdx_byStdsorted ===>" + orderedIdx_byStd)

    not_orderedIdx_byStd = not_orderedIdx_byStd.sorted
    // println("not_orderedIdx_byStd ===>" + not_orderedIdx_byStd)

    for(i<-0 until not_orderedIdx_byStd.size){
      star_point_List = star_point_List.updated(not_orderedIdx_byStd(i), -1)
    }

    for(i<-0 until orderedIdx_byStd.size){ // orderedIdx_byStd 크기 (학번당 들은 중분류를 for문 돌림)
      var k=0;
      //print(k)
      // 학과 전체의 중분류 리스트와 학생의 중분류 리스트의 값이 같을때까지 k를 증가
      while(sbjtCD_in_departNM_List(orderedIdx_byStd(i))!= valueBystdNo_from_Map(k).sbjtCD){
      k+=1;
      }
      // 같은 값이 나오면 0으로 설정돼있던 값을 (그 자리의 값을) 학생의 별점으로 바꿔줌
      star_point_List = star_point_List.updated(orderedIdx_byStd(i), valueBystdNo_from_Map(k).starpoint)
      // println(s"$star_point_List")
    }
  }
  val star_list = star_point_List.map(x => x.toString.toDouble)
  // println(">>"+star_list)
  sbjt_tuples = sbjt_tuples :+ (stdNo.toString, star_list)
}
