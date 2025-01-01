import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

object GraphframesTest{

  def main(args: Array[String]): Unit = {
    // Initialize a Spark session to execute Spark operations
    val spark = SparkSession.builder
      .master("local[*]")
      .getOrCreate()

    // Load vertices (customers and merchants) from CSV files
    val customers = getVertices(spark, "customer")
    val merchants = getVertices(spark, "merchant")
    val vertices = customers.union(merchants)

    // Load edges (transactions) from a CSV file
    val edges = getEdges(spark)

    // Create a graph using the vertices and edges
    val graph = GraphFrame(vertices, edges)

    // Step 1: Filter edges with the status "Disputed"
    val disputedEdges = graph.edges.filter(col("status") === "Disputed")

    // Step 2: Extract relevant information from the disputed edges
    val dstColRenames = Map("id" -> "dstId", "name" -> "dstName")
    val result = disputedEdges.join(graph.vertices, disputedEdges("src") === graph.vertices("id"))
      .join(graph.vertices.withColumnsRenamed(dstColRenames), disputedEdges("dst") === col("dstId"))
      .select(col("name"), col("dstName"), col("amount"), col("time"))

    // Step 3: Sort the extracted information by transaction time in descending order
    val sortedResult = result.orderBy(desc("time"))

    // Print or process the sorted result
    sortedResult.collect().foreach { row =>
      println(s"Customer Name: ${row.getAs[String]("name")}, Store Name: ${row.getAs[String]("dstName")}, Amount: ${row.getAs[Double]("amount")}, Transaction Time: ${row.getAs[String]("time")}")
    }

    // Stop the Spark session to free resources
    spark.stop()
  }

  // Helper function to load vertices (customers or merchants) from a CSV file
  private def getVertices(spark: SparkSession, entity: String): DataFrame = {
    spark.read
      .option("header", "true")
      .csv(s"data/${entity}s.csv")
      .select(col("id"), col("name"))
  }

  // Helper function to load edges (transactions) from a CSV file
  private def getEdges(spark: SparkSession): DataFrame = {
    spark.read.option("header", "true")
      .csv("data/transactions.csv")
      .withColumn("amount", col("amount").cast("double"))
      .select(col("customer_id").alias("src"), col("merchant_id").alias("dst"), col("amount"), col("time"),col("status"))
  }
}
