import java.io._
import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]) {
    val file = new File("/mnt/c/Users/Aditya Chatterjee/Desktop/graphx/output.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    val logFile = "/opt/spark/README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    bw.write(s"Lines with a: $numAs, Lines with b: $numBs")
    bw.close()
    spark.stop()
  }
}
