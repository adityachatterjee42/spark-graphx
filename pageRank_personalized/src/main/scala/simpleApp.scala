import java.io._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

object SimpleApp {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Simple App"))
    val g = GraphLoader.edgeListFile(sc, "Cit-HepTh.txt")
    val pr = g.personalizedPageRank(9207016, 0.001)
    .vertices
    .filter(_._1 != 9207016)
    .reduce((a,b) => if (a._2 > b._2) a else b)
    println("*************************************")
    println(pr)
    println("*************************************")
    sc.stop()
  }
}
