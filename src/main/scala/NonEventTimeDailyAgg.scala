import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object NonEventTimeDailyAgg {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("StructuredStreamingParquet")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4") // Reduce number of shuffle partitions
      .getOrCreate()

      import spark.implicits._

      // ✅ Define Schema for CSV Data
      val eventSchema = new StructType()
        .add("user_id", StringType)
        .add("session_id", StringType)
        .add("pageview_id", StringType)
        .add("session_duration", LongType)
        .add("event_type", StringType)
        .add("event_time", StringType)

    // ✅ Read CSV files in micro-batches
    val inputDF = spark.readStream
      .option("header", "true")
      .schema(eventSchema)
      .option("maxFilesPerTrigger", 2)  // ✅ Read 2 files per micro-batch
      .csv("/Users/mihirbose/IdeaProjects/HDFSBatchJob/src/data/csv_source_data")
      .repartition(2) // Control parallelism

    // ✅ Perform aggregation (non-event-time based)
    val aggregatedDF = inputDF
      .groupBy("user_id", "session_id")
      .agg(
        count("*").alias("total_pageviews"),
        sum("session_duration").alias("total_duration")
      )


    // ✅ Define foreachBatch function to write Parquet files
    def writeToParquet(batchDF: org.apache.spark.sql.DataFrame, batchId: Long): Unit = {
      val outputPath = s"/Users/mihirbose/IdeaProjects/HDFSBatchJob/src/data/non_event_agg_output/batch_$batchId"
      batchDF.write.mode("overwrite").parquet(outputPath)
      println(s"✅ Wrote batch $batchId to $outputPath")
    }

    // ✅ Write to plain Parquet files (NO saveAsTable, just files)
    val query = aggregatedDF
      .coalesce(1)
      .writeStream
      .outputMode("complete") // Full aggregation per batch
      .foreachBatch((df, batchId) => writeToParquet(df, batchId)) // ✅ Custom batch processing
//      .option("path", "/Users/mihirbose/IdeaProjects/HDFSBatchJob/src/data/non_event_agg_output") // Specify output directory
      .option("checkpointLocation", "/Users/mihirbose/IdeaProjects/HDFSBatchJob/src/data/checkpoints") // Needed for state tracking
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    query.awaitTermination(60000)

//    // Keep Spark UI running for 10 minutes (600000 milliseconds)
//    Thread.sleep(6000000)

    // Stop SparkSession manually when done
    spark.stop()
  }
}
