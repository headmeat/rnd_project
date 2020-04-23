// mongodb collection 제거
// db.USER_SIMILARITY.drop()

var userSimilarity_df = userSimilarity_table.select(col("STD_NO"),  col("SUBJECT_STAR"), col("NCR_STAR"), col("ACTING_COUNT"))
userSimilarity_df = userSimilarity_df.drop("_id")

//질의자
var querySTD_NO = 20142820
var querySTD = userSimilarity_df.filter(userSimilarity_df("STD_NO").equalTo(s"${querySTD_NO}")).drop("STD_NO")

val exStr = "WrappedArray|\\(|\\)|\\]|\\["
var querySTD_List = querySTD.collect.toList.mkString.replaceAll(exStr, "").split(",").map(x => (x.trim.toDouble * 10).toInt)

object CosineSimilarity {
   def dotProduct(x: Array[Int], y: Array[Int]): Int = {
     (for((a, b) <- x zip y) yield a * b) sum
   }
   def magnitude(x: Array[Int]): Double = {
     math.sqrt(x map(i => i*i) sum)
   }
  def cosineSimilarity(x: Array[Int], y: Array[Int]): Double = {
    require(x.size == y.size)
    dotProduct(x, y)/(magnitude(x) * magnitude(y))
  }
}

var sim_tuples = Seq[(String, Double)]()
var stdNO_inDepart_List = userSimilarity_df.select(col("STD_NO")).rdd.map(x => x(0)).collect().toList

stdNO_inDepart_List.foreach(stdNO => {
  println(stdNO)
  //유사사용자
  var std_inDepart = userSimilarity_df.filter(userSimilarity_df("STD_NO").equalTo(s"${stdNO}")).drop("STD_NO")
  var std_inDepart_List = std_inDepart.collect.toList.mkString.replaceAll(exStr, "").split(",").map(x => (x.trim.toDouble * 10).toInt)

  val sim = CosineSimilarity.cosineSimilarity(querySTD_List, std_inDepart_List)
  println(sim)
  sim_tuples = sim_tuples :+ (s"${stdNO}", sim)
})
var sim_df = sim_tuples.toDF("STD_NO", "similarity")





std1_List
Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
   0, 0, 0, 41, 0, 0, 0, 0, 0, 0, 44, 34, 33, 36, 45, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 45, 0, 38, 42, 40, 38, 0, 0, 0, 10, 0, 10, 10)

std2_List
Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 34, 44, 42, 36, 35, 0, 0, 0, 0, 0, 0, 0,
   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 38,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 40, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 50, 29, 0, 39, 0, 47, 48, 0, 10, 10, 0,
     10, 0)

std3_List
Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 35, 30, -10, 27, 42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 31, 40, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 41, 36, 0, 0, 0, 40, 27, 34, 20, 0,
  20, 0, 0)