
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import spark.implicits._


case class Rating(STD_NO: Int, SBJT_KEY_CD: String, rating: Float)
def parseRating(str: String): Rating = {
  val fields = str.split("::")
  assert(fields.size == 3)
  Rating(fields(0).toInt, fields(1).toString, fields(2).toFloat)
}

val ratings = spark.read.textFile("/home/rnd/Apps/sample_movielens_ratings1.txt").map(parseRating)
  .toDF()


val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

// Build the recommendation model using ALS on the training data
val als = new ALS()
  .setMaxIter(5)
  .setRegParam(0.01)
  .setUserCol("STD_NO")
  .setItemCol("SBJT_KEY_CD")
  .setRatingCol("rating")
val model = als.fit(training)

// Evaluate the model by computing the RMSE on the test data
// Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
model.setColdStartStrategy("drop")
val predictions = model.transform(test)

val evaluator = new RegressionEvaluator()
  .setMetricName("rmse")
  .setLabelCol("rating")
  .setPredictionCol("prediction")
val rmse = evaluator.evaluate(predictions)
println(s"Root-mean-square error = $rmse")

// Generate top 10 movie recommendations for each user
val userRecs = model.recommendForAllUsers(10)
// Generate top 10 user recommendations for each movie
val movieRecs = model.recommendForAllItems(10)
