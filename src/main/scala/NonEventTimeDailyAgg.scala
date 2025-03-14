import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.jdk.CollectionConverters.{IterableHasAsJava, IteratorHasAsScala}

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
      if (batchDF.isEmpty) {
        println(s"Stopping query as batch $batchId contains no data.")
        spark.streams.active.foreach(_.stop()) // Stop all active queries
      } else {
        // Get today's date
        val today = spark.sql("SELECT current_date()").first().getDate(0).toString

        // Write the joined DataFrame to Parquet files partitioned by event_date.
        val outputPath = "/Users/mihirbose/IdeaProjects/HDFSBatchJobs/src/data/non_event_agg_output"
        batchDF.write
          .mode("overwrite")
          .parquet(s"$outputPath/$today/batch_$batchId")

        println(s"✅ Wrote batch $batchId to $outputPath partitioned by event_date")
      }
    }

    // ✅ Write to plain Parquet files (NO saveAsTable, just files)
    val query = aggregatedDF
      .coalesce(1)
      .writeStream
      .outputMode("complete") // Full aggregation per batch
      .foreachBatch((batchDF: org.apache.spark.sql.DataFrame, batchId: Long) => {
        writeToParquet(batchDF, batchId)
      }) // ✅ Custom batch processing
      .option("checkpointLocation", "/Users/mihirbose/IdeaProjects/HDFSBatchJobs/src/data/checkpoints") // Needed for state tracking
      .trigger(Trigger.ProcessingTime("5 seconds"))
//      .trigger(Trigger.AvailableNow())
      .start()

    query.awaitTermination(60000)

//    // Keep Spark UI running for 10 minutes (600000 milliseconds)
//    Thread.sleep(6000000)

    // Stop SparkSession manually when done
    spark.stop()
  }
}
