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
    
    val initialMsg = 0

    
    val distGraph = graph.pregel(initialMsg, 
                            Int.MaxValue, 
                            EdgeDirection.Out)(
                            (id:VertexId,vd:Int,a:Int) => math.max(vd, a),
                            (et:EdgeTriplet[Int,String]) => Iterator((et.dstId, et.srcAttr+1)),
                            (a:Int,b:Int) => math.max(a,b)
                            )
    
    println("************************************************************************************")
    distGraph.vertices.collect.foreach{case (vertexId, (value, original_value)) => println(value)}
    println("************************************************************************************")
    //spark.stop()
  }
}
