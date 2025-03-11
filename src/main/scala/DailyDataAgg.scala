//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.hadoop.conf.Configuration
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
//import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, Trigger}
//
////This Spark Structured Streaming job processes large parquet files in micro-batches using stateful aggregation and schema validation. It performs two different aggregations, joins them, and writes the final result to Parquet.
////
////🔹 Key Features
////✅ Schema Validation → Ensures input data has the correct structure using Dataset[EventData].
////✅ Incremental Processing → Uses .option("maxFilesPerTrigger", 10) to process 10 files per batch.
////✅ Stateful Aggregation → Tracks user metrics (sessions, pageviews, duration, ad clicks) across micro-batches using GroupState.
////✅ Secondary Aggregation → Computes event type counts per user.
////✅ Join Operation → Merges both aggregated datasets on user_id.
////✅ Efficient Output Writing → Uses Update Mode, writing only updated records to Parquet.
////✅ Optimized Processing → Uses .trigger(Trigger.Once()) to write out the final result after all micro batches have been processed.
//
//
//object DailyDataAgg {
//
//  // ✅ Define case class for schema validation
//  case class EventData(
//                        user_id: String,
//                        session_id: String,
//                        pageview_id: String,
//                        session_duration: Long,
//                        event_type: String,
//                        event_time: String
//                      )
//
//  // ✅ Define case class for Aggregated Data
//  case class UserState(
//                        user_id: String,
//                        total_sessions: Long,
//                        total_pageviews: Long,
//                        total_duration: Long,
//                        ad_clicks: Long
//                      )
//
//  // ✅ Define case class for Per-User Event Type Count
//  case class EventTypeCount(
//                             user_id: String,
//                             event_type: String,
//                             event_count: Long
//                           )
//
//  def main(args: Array[String]): Unit = {
//
//    // ✅ Create Spark Session
//    val spark = SparkSession.builder()
//      .appName("Stateful Parquet Processing with Schema Validation")
//      .master("local[*]")
//      .getOrCreate()
//
//    import spark.implicits._
//
//    // ✅ Define Schema for CSV Data
//    val eventSchema = new StructType()
//      .add("user_id", StringType)
//      .add("session_id", StringType)
//      .add("pageview_id", StringType)
//      .add("session_duration", LongType)
//      .add("event_type", StringType)
//      .add("event_time", StringType)
//
//    // ✅ Step 1: Read Incrementally from CSV Files with Schema Validation
//    val rawData: Dataset[EventData] = spark.readStream
//      .format("csv")
//      .schema(eventSchema)  // ✅ Explicitly set schema
//      .option("header", "true")
//      .option("inferSchema", "true")
//      .option("path", "/Users/mihirbose/IdeaProjects/HDFSBatchJob/src/data/csv_source_data")
//      .option("maxFilesPerTrigger", 2)  // ✅ Read 2 files per micro-batch
//      .load()
//      .withColumn("event_time", col("event_time").cast(TimestampType)) // ✅ Ensure correct type
//      .as[EventData] // ✅ Enforce Schema Validation
//
//    // ✅ Step 2: Apply Window Function and Extract Start Time
//    val rawDataWithWindow = rawData
//      .withColumn("window", window(col("event_time"), "10 minutes")) // ✅ Create window struct
//      .withColumn("event_time", col("window.start")) // ✅ Extract start time
//      .drop("window") // ✅ Drop the struct if not needed
//
//    // ✅ Step 3: Define Stateful Aggregation Function
//    val updateStateFunc = (userId: String, batchData: Iterator[EventData], state: GroupState[UserState]) => {
//      val existingState = state.getOption.getOrElse(UserState(userId, 0, 0, 0, 0))
//
//      val batchTotalSessions = batchData.map(_.session_id).toSet.size.toLong
//      val batchTotalPageviews = batchData.size.toLong
//      val batchTotalDuration = batchData.map(_.session_duration).sum
//      val batchAdClicks = batchData.count(_.event_type == "ad_click")
//
//      val updatedState = UserState(
//        userId,
//        existingState.total_sessions + batchTotalSessions,
//        existingState.total_pageviews + batchTotalPageviews,
//        existingState.total_duration + batchTotalDuration,
//        existingState.ad_clicks + batchAdClicks
//      )
//
//      state.update(updatedState)
//      updatedState  // ✅ Return UserState instead of an Iterator
//    }
//
//
//    implicit val stringEncoder = Encoders.STRING // ✅ Encoder for user_id (String)
//    implicit val userStateEncoder = Encoders.product[UserState] // ✅ Encoder for UserState
//
//    // ✅ Step 4: Perform Stateful Aggregation
//    val userAggregates: Dataset[UserState] = rawDataWithWindow
//      .groupByKey(_.user_id)
//      .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout())(updateStateFunc)
//
//    // ✅ Step 5: Compute Event Type Counts
//    val eventTypeAggregates: Dataset[EventTypeCount] = rawDataWithWindow
//      .groupByKey(event => (event.user_id, event.event_type))
//      .count()
//      .map { case ((userId, eventType), count) => EventTypeCount(userId, eventType, count) }
//
//    // ✅ Step 6: Perform a Join Between Both Aggregations
//    val joinedData = userAggregates
//      .joinWith(eventTypeAggregates, userAggregates("user_id") === eventTypeAggregates("user_id"))
//
//    // ✅ Step 7: Write Final Result to Parquet
//    val query = rawDataWithWindow
//      .coalesce(1) // ✅ Force single output file
//      .writeStream
//      .outputMode("append")  // ✅ Only write updated rows
//      .format("parquet")
//      .option("path", "/Users/mihirbose/IdeaProjects/HDFSBatchJob/src/data/output_data")
//      .option("checkpointLocation", "/Users/mihirbose/IdeaProjects/HDFSBatchJob/src/data/checkpoints/")
//      .trigger(Trigger.ProcessingTime("1 seconds"))
//      .start()
//
//    query.awaitTermination(10000)
//  }
//}
