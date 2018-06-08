import java.io._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

object SimpleApp {
  def main(args: Array[String]) {
    //val file = new File("/mnt/c/Users/Aditya Chatterjee/Desktop/graphx/output.txt")
    //val bw = new BufferedWriter(new FileWriter(file))
    val sc = new SparkContext(new SparkConf().setAppName("Simple App"))
    val vertices = sc.parallelize(Array((1L, (7,-1)), (2L, (3,-1)), (3L, (2,-1)), (4L, (6,-1))))
    val relationships = sc.parallelize(Array(Edge(1L, 2L, true), Edge(1L, 4L, true), Edge(2L, 4L, true), Edge(3L, 1L, true), Edge(3L, 4L, true)))
    val graph = Graph(vertices, relationships)
    println("************************************************************************************")
    graph.vertices.collect.foreach(println)
    println("************************************************************************************")
    val initialMsg = 9999

    def vprog(vertexId: VertexId, value: (Int, Int), message: Int): (Int, Int) = {
      if (message == initialMsg)
        value
      else
        (message min value._1, value._1)
    }

    def sendMsg(triplet: EdgeTriplet[(Int, Int), Boolean]): Iterator[(VertexId, Int)] = {
      val sourceVertex = triplet.srcAttr

      if (sourceVertex._1 == sourceVertex._2)
        Iterator.empty
      else 
        Iterator((triplet.dstId, sourceVertex._1))
    }

    def mergeMsg(msg1: Int, msg2: Int): Int = msg1 min msg2
    //val logData = spark.read.textFile(logFile).cache()
    //val numAs = logData.filter(line => line.contains("a")).count()
    //val numBs = logData.filter(line => line.contains("b")).count()
    //bw.write(s"Lines with a: $numAs, Lines with b: $numBs")
    //bw.close()

    val minGraph = graph.pregel(initialMsg, 
                            Int.MaxValue, 
                            EdgeDirection.Out)(
                            vprog,
                            sendMsg,
                            mergeMsg)
    
    println("************************************************************************************")
    minGraph.vertices.collect.foreach{case (vertexId, (value, original_value)) => println(value)}
    println("************************************************************************************")
    //spark.stop()
  }
}
