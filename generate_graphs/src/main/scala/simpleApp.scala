import java.io._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

object SimpleApp {
  def toGexf[VD,ED](g:Graph[VD,ED]) =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
    "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
    "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
    "    <nodes>\n" +
    g.vertices.map(v => "      <node id=\"" + v._1 + "\" label=\"" +
                        v._2 + "\" />\n").collect.mkString +
    "    </nodes>\n" +
    "    <edges>\n" +
    g.edges.map(e => "      <edge source=\"" + e.srcId +
                     "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
                     "\" />\n").collect.mkString +
    "    </edges>\n" +
    "  </graph>\n" +
    "</gexf>"

  def main(args: Array[String]) {
    //val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sc = new SparkContext(new SparkConf().setAppName("Simple App"))
    var pw = new java.io.PrintWriter("gridGraph.gexf")
    pw.write(toGexf(util.GraphGenerators.gridGraph(sc, 4, 4)))
    pw.close
    pw = new java.io.PrintWriter("starGraph.gexf")
    pw.write(toGexf(util.GraphGenerators.starGraph(sc, 8)))
    pw.close
    val logNormalGraph = util.GraphGenerators.logNormalGraph(sc, 15)
    pw = new java.io.PrintWriter("logNormalGraph.gexf")
    pw.write(toGexf(logNormalGraph))
    pw.close
    pw = new java.io.PrintWriter("rmatGraph.gexf")
    pw.write(toGexf(util.GraphGenerators.rmatGraph(sc, 32, 60)))
    pw.close
    sc.stop()

  }
}
