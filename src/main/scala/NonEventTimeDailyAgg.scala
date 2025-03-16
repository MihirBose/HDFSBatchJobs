import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * This application represents a spark batch job that aggregates data in micro-batch manner using
 * Spark's Structured Streaming.
 *
 * It reads only a specific number of files(maxFilesPerTrigger) from source (Any file source like HDFS or local
 * for testing), aggregates the data of the current micro-batch with the result set of the aggregations of all the
 * micro batches so far, as we are using the 'complete' mode.
 *
 * That output is then sent to a method using forEachBatch which writes the result to an output directory.
 */

object NonEventTimeDailyAgg {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("StructuredStreamingParquet")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4") // Reduce number of shuffle partitions
      .getOrCreate()

    import spark.implicits._

    // Define Schema for CSV Data
    val eventSchema = new StructType()
      .add("user_id", StringType)
      .add("session_id", StringType)
      .add("pageview_id", StringType)
      .add("session_duration", LongType)
      .add("event_type", StringType)
      .add("event_time", StringType)

    // Read CSV files in micro-batches
    val inputDF = spark.readStream
      .option("header", "true")
      .schema(eventSchema)
      .option("maxFilesPerTrigger", 2)
      .csv("/Users/mihirbose/IdeaProjects/HDFSBatchJobs/src/data/csv_source_data")
      .repartition(2) // Control parallelism


    // Perform aggregation on non-time columns
    // Grouping by user_id and session_id only.
    val aggregatedDF = inputDF
      .groupBy("user_id")
      .agg(
        count("*").alias("total_pageviews"),
        sum("session_duration").alias("total_duration"),
        count(when(col("event_type") === "page_view", true)).alias("total_page_views"),
        count(when(col("event_type") === "ad_click", true)).alias("total_ad_clicks"),
        count(when(col("event_type") === "form_submit", true)).alias("total_form_submits"),
        count(when(col("event_type") === "video_play", true)).alias("total_video_plays"),
        count(when(col("event_type") === "purchase", true)).alias("total_purchases")
      )


    // 5. Define foreachBatch function to write Parquet files partitioned by event_date.
    //    We join back with a static DataFrame that holds (user_id, session_id, event_date)
    def writeToParquet(batchDF: org.apache.spark.sql.DataFrame, batchId: Long): Unit = {
        // Get today's date
        val today = spark.sql("SELECT current_date()").first().getDate(0).toString

        // Write the joined DataFrame to Parquet files partitioned by event_date.
        val outputPath = "/Users/mihirbose/IdeaProjects/HDFSBatchJobs/src/data/non_event_agg_output"
        batchDF.write
          .mode("overwrite")
          .parquet(s"$outputPath/$today")

        println(s"Wrote batch $batchId to $outputPath")
      }


    // Write to plain Parquet files (NO saveAsTable, just files)
    val query = aggregatedDF
      .coalesce(1)
      .writeStream
      .outputMode("complete") // Full aggregation per batch
      .foreachBatch((batchDF: org.apache.spark.sql.DataFrame, batchId: Long) => {
        writeToParquet(batchDF, batchId)
      }) // Custom batch processing
      .option("checkpointLocation",
        "/Users/mihirbose/IdeaProjects/HDFSBatchJobs/src/data/checkpoints/non_event_time_checkpoints") // Needed for state tracking
      .trigger(Trigger.AvailableNow())
      .start()

    query.awaitTermination()

//    // Keep Spark UI running for 10 minutes (600000 milliseconds)
//    Thread.sleep(6000000)

    // Stop SparkSession manually when done
    spark.stop()
  }
}
