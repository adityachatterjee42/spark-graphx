import java.io._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

object SimpleApp {
  def main(args: Array[String]) {
    val file = new File("/mnt/c/Users/Aditya Chatterjee/Desktop/graphx/output.txt")
    val sc = new SparkContext(new SparkConf().setAppName("Simple App"))
    val bw = new BufferedWriter(new FileWriter(file))
    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"), (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"), Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))
    val myGraph = Graph(myVertices, myEdges)
    val vertices = myGraph.vertices.collect
    val strings = vertices.map(_.productIterator.mkString(" ")+"\t")
    strings.foreach { bw.write }
    //bw.write(myGraph.vertices.collect)
    bw.close()
  }
}
