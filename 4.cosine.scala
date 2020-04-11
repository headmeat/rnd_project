public class CosineSimilarity extends AbstractSimilarity {

  @Override
  protected double computeSimilarity(Matrix sourceDoc, Matrix targetDoc) {
    double dotProduct = sourceDoc.arrayTimes(targetDoc).norm1();
    double eucledianDist = sourceDoc.normF() * targetDoc.normF();
    return dotProduct / eucledianDist;
  }
}





public static double cosineSimilarity(double[] vectorA, double[] vectorB) {
    double dotProduct = 0.0;
    double normA = 0.0;
    double normB = 0.0;
    for (int i = 0; i < vectorA.length; i++) {
        dotProduct += vectorA[i] * vectorB[i];
        normA += Math.pow(vectorA[i], 2);
        normB += Math.pow(vectorB[i], 2);
    }
    return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
}
//-----------------java----------------



def similarity(t1: Map[String, Int], t2: Map[String, Int]): Double = {
    //word, t1 freq, t2 freq
    val m = scala.collection.mutable.HashMap[String, (Int, Int)]()

    val sum1 = t1.foldLeft(0d) {case (sum, (word, freq)) =>
        m += word ->(freq, 0)
        sum + freq
    }

    val sum2 = t2.foldLeft(0d) {case (sum, (word, freq)) =>
        m.get(word) match {
            case Some((freq1, _)) => m += word ->(freq1, freq)
            case None => m += word ->(0, freq)
        }
        sum + freq
    }

    val (p1, p2, p3) = m.foldLeft((0d, 0d, 0d)) {case ((s1, s2, s3), e) =>
        val fs = e._2
        val f1 = fs._1 / sum1
        val f2 = fs._2 / sum2
        (s1 + f1 * f2, s2 + f1 * f1, s3 + f2 * f2)
    }

    val cos = p1 / (Math.sqrt(p2) * Math.sqrt(p3))
    cos
}
