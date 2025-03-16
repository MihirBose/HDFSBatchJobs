import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, Trigger}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StatefulAgg {

   case class UserEventState(
                             user_id: String,
                             total_duration: Long,
                             total_pageviews: Long,
                             total_page_views: Long,
                             total_ad_clicks: Long,
                             total_form_submits: Long,
                             total_video_plays: Long,
                             total_purchases: Long
                           )

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("StatefulAggregation")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    import sparkSession.implicits._

    val eventSchema = new StructType()
      .add("user_id", StringType)
      .add("session_id", StringType)
      .add("pageview_id", StringType)
      .add("session_duration", LongType)
      .add("event_type", StringType)
      .add("event_time", StringType)

    // ✅ Read CSV files in micro-batches (NO WATERMARKING)
    val inputDF = sparkSession.readStream
      .option("header", "true")
      .schema(eventSchema)
      .option("maxFilesPerTrigger", 1)  // ✅ Read 2 files per micro-batch
      .csv("/Users/mihirbose/IdeaProjects/HDFSBatchJobs/src/data/test_data")

    implicit val userEventStateEncoder: Encoder[UserEventState] = Encoders.product[UserEventState]

    // ✅ Update Function for State Management
    def updateUserState(
                         user_id: String,
                         events: Iterator[Row],
                         state: GroupState[UserEventState]
                       ): UserEventState = {

      val newEvents = events.toSeq

      val updatedState = state.getOption.getOrElse(
        UserEventState(user_id, 0, 0, 0, 0, 0, 0, 0)
      )

      val newState = updatedState.copy(
        total_duration = updatedState.total_duration + newEvents.map(_.getAs[Long]("session_duration")).sum,
        total_pageviews = updatedState.total_pageviews + newEvents.size,
        total_page_views = updatedState.total_page_views + newEvents.count(_.getAs[String]("event_type") == "page_view"),
        total_ad_clicks = updatedState.total_ad_clicks + newEvents.count(_.getAs[String]("event_type") == "ad_click"),
        total_form_submits = updatedState.total_form_submits + newEvents.count(_.getAs[String]("event_type") == "form_submit"),
        total_video_plays = updatedState.total_video_plays + newEvents.count(_.getAs[String]("event_type") == "video_play"),
        total_purchases = updatedState.total_purchases + newEvents.count(_.getAs[String]("event_type") == "purchase")
      )

      state.update(newState)
      newState
    }

    // ✅ Apply Stateful Aggregation (NO WATERMARK)
    val aggregatedDF = inputDF
      .groupByKey(_.getAs[String]("user_id"))
      .mapGroupsWithState(GroupStateTimeout.NoTimeout())(updateUserState)
      .toDF()

    // ✅ Define foreachBatch function to update POSTGRES
    // TODO: Rewrite this method so it writes to POSTGRES
    def writeToParquet(batchDF: DataFrame, batchId: Long): Unit = {
      if (batchDF.isEmpty) {
        println(s"Stopping query as batch $batchId contains no data.")
        sparkSession.streams.active.foreach(_.stop()) // Stop all active queries
      } else {
        val outputPath = "/Users/mihirbose/IdeaProjects/HDFSBatchJobs/src/data/stateful_output"
        batchDF.write
          .mode("append")
          .parquet(s"$outputPath/batch_$batchId")

        println(s"✅ Wrote batch $batchId to $outputPath")
      }
    }

    // ✅ Write to Parquet
    val query = aggregatedDF
      .coalesce(1)
      .writeStream
      .outputMode("update") // Use update mode instead of complete
      .option("checkpointLocation", "/Users/mihirbose/IdeaProjects/HDFSBatchJobs/src/data/stateful_checkpoints")
      .trigger(Trigger.AvailableNow())
      .foreachBatch((batchDF: org.apache.spark.sql.DataFrame, batchId: Long) => {
        writeToParquet(batchDF, batchId)
      }) // ✅ Custom batch processing
      .start()

    query.awaitTermination()

    // Stop SparkSession manually when done
    sparkSession.stop()
  }
}
