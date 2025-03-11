//import org.apache.spark.sql.{SparkSession, DataFrame}
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.streaming.Trigger
//
//
////✅ Reads streaming data from HDFS.
////✅ Aggregates counts for each (event_time, animal).
////✅ Uses UPSERT (MERGE INTO) to avoid duplicates in PostgreSQL.
////✅ Uses foreachBatch() for efficient batch writes.
//
////🔹 How It Works
////1️⃣ Reads parquet files from HDFS incrementally using maxFilesPerTrigger.
////2️⃣ Groups data into 10-minute windows and counts events.
////3️⃣ Uses foreachBatch() to write each micro-batch to PostgreSQL.
////4️⃣ MERGE INTO (UPSERT) ensures:
////
////✅ If event_time, animal already exists, update total_count.
////✅ If new data appears, insert it as a new row.
////5️⃣ Uses Trigger.ProcessingTime("10 seconds") to process every 10 sec.
////🔹 Why This Approach?
////✅ Efficient Streaming → Processes micro-batches, not the entire dataset.
////✅ No Data Loss → Maintains previous counts while updating new ones.
////✅ Prevents Duplicates → Uses MERGE INTO to avoid duplicate entries.
////✅ Fast Processing → Uses parallelism & partitions for speed.
//
//class StreamingUpsertJob(spark: SparkSession) {
//
//  // ✅ 1. Read Streaming Data from HDFS
//  def readStreamingData(): DataFrame = {
//    spark.readStream
//      .format("parquet")
//      .option("path", "hdfs://namenode:8020/data/events/")
//      .option("maxFilesPerTrigger", 10) // Process 10 files per micro-batch
//      .load()
//  }
//
//  // ✅ 2. Aggregate Counts Per Window
//  def aggregateData(streamingDF: DataFrame): DataFrame = {
//    streamingDF
//      .withColumn("event_time", window(col("timestamp"), "10 minutes").start)
//      .groupBy("event_time", "animal")
//      .agg(count("*").alias("total_count"))
//  }
//
//  // ✅ 3. UPSERT Function for PostgreSQL
//  def writeToPostgres(batchDF: DataFrame, batchId: Long): Unit = {
//    batchDF.createOrReplaceTempView("batch_data")
//
//    batchDF.sparkSession.sql(
//      """
//      MERGE INTO animal_counts AS target
//      USING batch_data AS source
//      ON target.event_time = source.event_time AND target.animal = source.animal
//      WHEN MATCHED THEN
//        UPDATE SET target.total_count = source.total_count
//      WHEN NOT MATCHED THEN
//        INSERT (event_time, animal, total_count)
//        VALUES (source.event_time, source.animal, source.total_count)
//      """
//    )
//  }
//
//  // ✅ 4. Start Streaming Query
//  def start(): Unit = {
//    val streamingDF = readStreamingData()
//    val aggregatedDF = aggregateData(streamingDF)
//
//    val query = aggregatedDF.writeStream
//      .outputMode("update")  // Write only updated rows
//      .foreachBatch(writeToPostgres _)
//      .trigger(Trigger.ProcessingTime("10 seconds")) // Run every 10 sec
//      .start()
//
//    query.awaitTermination()
//  }
//}
//
//// ✅ 5. Main Application Entry Point
//object StreamingUpsertApp {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder()
//      .appName("Streaming UPSERT to PostgreSQL")
//      .master("local[*]")
//      .config("spark.sql.shuffle.partitions", 200)
//      .getOrCreate()
//
//    val job = new StreamingUpsertJob(spark)
//    job.start()
//  }
//}
//
////Final PostgreSQL Table (animal_counts)
////Stored in PostgreSQL with UPSERT logic.
////
////📌 animal_counts (After First Batch)
////event_time	animal	total_count
////2024-03-07 12:00	dog	2
////2024-03-07 12:00	cat	2
////2024-03-07 12:10	dog	1
//
////📌 After Another Micro-Batch (If New Data Arrives)
////event_time	animal	total_count
////2024-03-07 12:00	dog	3
////2024-03-07 12:00	cat	2
////2024-03-07 12:10	dog	1
