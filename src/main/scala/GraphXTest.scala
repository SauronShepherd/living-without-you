import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphXTest{

  // Define the vertex properties as a case class to store vertex-related information
  private case class VertexProperty(name: String)

  // Define the edge properties as a case class to store edge-related information
  private case class EdgeProperty(amount: Double, time: String, status: String)

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
    val graph = Graph(vertices, edges)

    // Step 1: Filter edges with the status "Disputed"
    val disputedEdges = graph.triplets.filter(triplet => triplet.attr.status == "Disputed")

    // Step 2: Extract relevant information from the disputed edges
    val result = disputedEdges.map {
      triplet => (triplet.srcAttr.name, triplet.dstAttr.name, triplet.attr.amount, triplet.attr.time)
    }

    // Step 3: Sort the extracted information by transaction time in descending order
    val sortedResult = result.sortBy(_._4, ascending = false)

    // Print or process the sorted result
    sortedResult.collect().foreach { case (customerName, storeName, amount, transactionTime) =>
      println(s"Customer Name: $customerName, Store Name: $storeName, Amount: $amount, Transaction Time: $transactionTime")
    }

    // Stop the Spark session to free resources
    spark.stop()
  }

  // Helper function to load vertices (customers or merchants) from a CSV file
  private def getVertices(spark: SparkSession, entity: String): RDD[(VertexId, VertexProperty)] = {
    spark.read
      .option("header", "true")
      .csv(s"data/${entity}s.csv")
      .select(col("id"), col("name"))
      .rdd.map(row => (row.getAs[String]("id").toLong, VertexProperty(row.getAs[String]("name"))))
  }

  // Helper function to load edges (transactions) from a CSV file
  private def getEdges(spark: SparkSession): RDD[Edge[EdgeProperty]] = {
    spark.read.option("header", "true")
      .csv("data/transactions.csv")
      .rdd.map(
        row => Edge(
          row.getAs[String]("customer_id").toLong, row.getAs[String]("merchant_id").toLong,
          EdgeProperty(row.getAs[String]("amount").toDouble, row.getAs[String]("time"), row.getAs[String]("status"))
        )
      )
  }
}
