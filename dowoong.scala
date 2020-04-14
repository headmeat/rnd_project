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
  }
//}

----------------------------------------------------협업필터링

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS

case class Rating(stdId: Int, pid: String, rating: Float, timestamp: Long)
def parseRating(str: String): Rating = {
  val fields = str.split("::")
  assert(fields.size == 4)
  Rating(fields(0).toInt, fields(1).toString, fields(2).toFloat, fields(3).toLong)
}



//DB에 저장된 교과/비교과/자율활동 리스트를 dataframe 형태로 변환
//1. DB에서 값 불러오기
//학생 하나에 대해 교과, 비교과, 자율활동 list가 존재
//학생 하나의 dataframe을 불러오면


//2. list를 dataframe으로 변환


//3. 교과, 비교과, 자율 각각 ratings를 생성 (컴공과 하나에 대한 document를 가져와서 dataframe으로 변환)
val ratings



val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

// Build the recommendation model using ALS on the training data
val als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("id").setItemCol("SIM_Result").setItemCol("TRUST_Result").setRatingCol("Result")
val model = als.fit(training)

// Evaluate the model by computing the RMSE on the test data
// Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
model.setColdStartStrategy("drop")
val predictions = model.transform(test)

val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")

val rmse = evaluator.evaluate(predictions)
println(s"Root-mean-square error = $rmse")

// Generate top 10 movie recommendations for each user
val userRecs = model.recommendForAllUsers(10)
// Generate top 10 user recommendations for each movie
val movieRecs = model.recommendForAllItems(10)

// Generate top 10 movie recommendations for a specified set of users
val users = ratings.select(als.getUserCol).distinct().limit(3)
val userSubsetRecs = model.recommendForUserSubset(users, 10)
// Generate top 10 user recommendations for a specified set of movies
val movies = ratings.select(als.getItemCol).distinct().limit(3)
val movieSubSetRecs = model.recommendForItemSubset(movies, 10)
================================================================================================================



package com.packt.ScalaML.MovieRecommendation
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import scala.Tuple2
import org.apache.spark.rdd.RDD


val ratigsFile = "data/ratings.csv"
val df1 = spark.read.format("com.databricks.spark.csv").option("header", true).load(ratigsFile)
val ratingsDF = df1.select(df1.col("userId"), df1.col("movieId"), df1.col("rating"), df1.col("timestamp"))
ratingsDF.show(false)


val moviesFile = "data/movies.csv"
val df2 = spark.read.format("com.databricks.spark.csv").option("header", "true").load(moviesFile)
val moviesDF = df2.select(df2.col("movieId"), df2.col("title"), df2.col("genres"))


ratingsDF.createOrReplaceTempView("ratings")
moviesDF.createOrReplaceTempView("movies")


val numRatings = ratingsDF.count()
val numUsers = ratingsDF.select(ratingsDF.col("userId")).distinct().count()
val numMovies = ratingsDF.select(ratingsDF.col("movieId")).distinct().count() println("Got " + numRatings + " ratings from " + numUsers + " users on " + numMovies + " movies.")


val results = spark.sql("select movies.title, movierates.maxr, movierates.minr, movierates.cntu "
+ "from(SELECT ratings.movieId,max(ratings.rating) as maxr,"
+ "min(ratings.rating) as minr,count(distinct userId) as cntu "
+ "FROM ratings group by ratings.movieId) movierates "
+ "join movies on movierates.movieId=movies.movieId "
+ "order by movierates.cntu desc") results.show(false)

val mostActiveUsersSchemaRDD = spark.sql("SELECT ratings.userId, count(*) as ct from ratings "+ "group by ratings.userId order by ct desc limit 10")mostActiveUsersSchemaRDD.show(false)


val results2 = spark.sql(
"SELECT ratings.userId, ratings.movieId,"
+ "ratings.rating, movies.title FROM ratings JOIN movies"
+ "ON movies.movieId=ratings.movieId"
+ "where ratings.userId=668 and ratings.rating > 4") results2.show(false)


val splits = ratingsDF.randomSplit(Array(0.75, 0.25), seed = 12345L)
val (trainingData, testData) = (splits(0), splits(1))
val numTraining = trainingData.count()
val numTest = testData.count()
println("Training: " + numTraining + " test: " + numTest)

val ratingsRDD = trainingData.rdd.map(row => {
val userId = row.getString(0)
val movieId = row.getString(1)
val ratings = row.getString(2)
Rating(userId.toInt, movieId.toInt, ratings.toDouble)
})

val testRDD = testData.rdd.map(row => {
val userId = row.getString(0)
val movieId = row.getString(1)
val ratings = row.getString(2)
Rating(userId.toInt, movieId.toInt, ratings.toDouble)
})


val rank = 20
val numIterations = 15
val lambda = 0.10
val alpha = 1.00 val block = -1
val seed = 12345L
val implicitPrefs = false
val model = new ALS().setIterations(numIterations) .setBlocks(block).setAlpha(alpha)
.setLambda(lambda)
.setRank(rank) .setSeed(seed)
.setImplicitPrefs(implicitPrefs)
.run(ratingsRDD)


println("Rating:(UserID, MovieID, Rating)")
println("----------------------------------")
val topRecsForUser = model.recommendProducts(668, 6) for (rating <- topRecsForUser) { println(rating.toString()) } println("----------------------------------") >>>

val rmseTest = computeRmse(model, testRDD, true)
println("Test RMSE: = " + rmseTest) //Less is better

def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean): Double = { val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product))) valpredictionsAndRatings = predictions.map { x => ((x.user, x.product), x.rating) } .join(data.map(x => ((x.user, x.product), x.rating))).values if (implicitPrefs) { println("(Prediction, Rating)") println(predictionsAndRatings.take(5).mkString("n")) } math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean()) }println("Recommendations: (MovieId => Rating)") println("----------------------------------") val recommendationsUser = model.recommendProducts(668, 6) recommendationsUser.map(rating => (rating.product, rating.rating)).foreach(println) println("----------------------------------")


println("Recommendations: (MovieId => Rating)") println("----------------------------------") val recommendationsUser = model.recommendProducts(668, 6) recommendationsUser.map(rating => (rating.product, rating.rating)).foreach(println) println("----------------------------------")
























------------------------------------------------------------------------------------------------------0413


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
