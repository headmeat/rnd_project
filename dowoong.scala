package org.example


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkException
//////////////////////
//object rnd_result {

//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder.master("local[*]").appName("Random").getOrCreate()
///////////////////////
    val sc = spark.sparkContext

    val a = double2Double(1)
    val b = double2Double(1)
    val w1 = 0.5
    val w2 = 0.5

    val sim = scala.util.Random

    case class myClass(id: Int, SIM: Double, TRUST: Double, Result: Double)
    import spark.implicits._


    //유사도팀에서 DB에 저장된 교과,비교과,자율 dataframe을 이용해서 학번 별 유사도 dataframe을 만들어서 줌
    //신뢰도 팀에서도 학번 별 신뢰도 dataframe을 줌
    //2개를 합쳐서 하나의 dataframe을 만들고 가중치를 주어 ranking값을 가진 새로운 dataframe을 생성

    val list0 = (1 to 100).map { x =>
      val tid = x
      val tSim = sim.nextDouble()
      val tTrust = sim.nextDouble()
      val tResult = 0
      val res = myClass(tid, tSim, tTrust, tResult)
      res
    }
    val myDF = list0.toDF()
    myDF.show()

    case class myClass2(id: Int, SIM_Result: Double, TRUST_Result: Double, Result: Double)
    val list1 = list0.map { row =>
      val c1 = row.SIM * a * w1
      val c2 = row.TRUST * b * w2
      val c3 = c1 + c2
      val res = myClass2(row.id, c1, c2, c3)
      res
    }

    val myDF2 = list1.toDF().orderBy(desc("Result"))
    myDF2.show()

    val myDF3 = list1.toDF().orderBy(desc("Result")).limit(10)
    myDF3.show()


//////////////////////////////////////////////////////////////////////졸업생 N명 선정후 많이 들은 순으로 출력
    /교과

    var clPassUri_res = clPassUri_table.select(col("STD_NO"), col("SUST_CD_NM"), col("SBJT_KEY_CD"), col("SBJT_KOR_NM")).distinct.toDF
    clPassUri_res.show()

    val std_arr = Seq(20190030, 20170063, 20142915, 20152634, 20142824)
    val std_sbjt_arr = std_arr.map{ stdno =>
    val res = clPassUri_res.filter(clPassUri_res("STD_NO").equalTo(s"${stdno}"))
    res
    }.map{ x =>
    val res = x.select("SBJT_KOR_NM").collect.toList.map( x=> x.toString).distinct
    res
    }.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

    val top5 = std_sbjt_arr.take(5)






    /비교과

    // 데이터 학번 20142820, 20142932, 20152611, 20152615

    var cpsStarUri_res = cpsStarUri_table.select(col("STD_NO"), col("STAR_KEY_ID"), col("YEAR"),col("SMESTR")).distinct.toDF
        cpsStarUri_res.show()


//  val dfs_2 = Seq(std_STAR_KEY_list1, std_STAR_KEY_list2, std_STAR_KEY_list3, std_STAR_KEY_list4)
    val std_arr2 = Seq(20142820, 20142932, 20152611, 20152615)
    val std_STAR_KEY_arr2 = std_arr2.map{ stdno =>
    val res = cpsStarUri_res.filter(cpsStarUri_res("STD_NO").equalTo(s"${stdno}"))
        res
        }.map{ x =>
    val res = x.select("STAR_KEY_ID").collect.toList.map( x=> x.toString)
        res
        }.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

    val top5 = std_STAR_KEY_arr2.take(5)




    /자율 활동

    var clPassUri_res = clPassUri_table.select(col("STD_NO"), col("SUST_CD_NM"), col("SBJT_KEY_CD"), col("SBJT_KOR_NM")).distinct.toDF
    clPassUri_res.show()

    val std_arr = (20190030, 20170063, 20142915, 20152634, 20142824)


    val dfs = Seq(std_sbjt_list1, std_sbjt_list2, std_sbjt_list3, std_sbjt_list4, std_sbjt_list5)
    val std_arr = Seq(20190030, 20170063, 20142915, 20152634, 20142824)
    val std_sbjt_arr = std_arr.map{ stdno =>
    val res = clPassUri_res.filter(clPassUri_res("STD_NO").equalTo(s"${stdno}"))
    res
    }.map{ x =>
    val res = x.select("SBJT_KOR_NM").rdd.map(r=>r(0)).collect.toList.map( x=> x.toString).distinct
    res
    }.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

    val top5 = std_sbjt_arr.take(5)
