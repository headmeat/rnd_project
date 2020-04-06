package org.example


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkException
//////////////////////
object rnd_result {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("Random").getOrCreate()
///////////////////////
    val sc = spark.sparkContext

    val a = double2Double(1)
    val b = double2Double(1)
    val w1 = 0.5
    val w2 = 0.5

    val sim = scala.util.Random

    case class myClass(id: Int, SIM: Double, TRUST: Double, Result: Double)
    import spark.implicits._
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
}
