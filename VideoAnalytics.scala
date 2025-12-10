import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.evaluation.RegressionEvaluator
import java.nio.file.Paths
import java.util.concurrent.CountDownLatch

object VideoAnalytics {

  def main(args: Array[String]): Unit = {

    // 1. Initialize Spark
    val spark = SparkSession.builder()
      .appName("Advanced Video Analytics")
      .master("local[*]")
      .config("spark.ui.enabled", "true")
      .config("spark.ui.port", "4040")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    import spark.implicits._

    // ---------------------------------------------------------
    // PATH FIX: Using Absolute Path
    // ---------------------------------------------------------
    // REPLACE "PC" with your actual Windows Username if different
    val absolutePath = "C:/Users/PC/OneDrive/Desktop/VideoWatchProject/data/video_logs.csv"

    println(s"DEBUG: Loading from: $absolutePath")

    val rawData = try {
       spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(absolutePath)
    } catch {
      case e: Exception =>
        println(s"CRITICAL ERROR: Spark could not find the file at: $absolutePath")
        println("Please check: 1. The file name is exactly video_logs.csv")
        println("              2. The file is in the 'data' folder on your Desktop")
        spark.stop()
        System.exit(1)
        null
    }
    // ---------------------------------------------------------

    println("--- 1. Running Pattern Analysis ---")

    // Task B: Skip Rate
    val videoMetrics = rawData
      .groupBy("video_id")
      .agg(
        sum(when($"event_type" === "SEEK_FORWARD", 1).otherwise(0)).alias("skip_count"),
        count("event_type").alias("total_interactions")
      )
      .withColumn("skip_rate", round($"skip_count" / $"total_interactions", 2))

    // Task C: Boredom Heatmap
    val boredomHeatmap = rawData
      .filter($"event_type" === "SEEK_FORWARD")
      .withColumn("time_bucket", (col("timestamp_in_video") / 30).cast("int") * 30)
      .groupBy("video_id", "time_bucket")
      .count()
      .withColumnRenamed("count", "num_skips")
      .withColumnRenamed("video_id", "videoId")

    println("--- 2. Feature: Smart Ad-Insertion ---")
    val safeZones = rawData
      .filter($"event_type" === "STOP")
      .withColumn("time_bucket", (col("timestamp_in_video") / 30).cast("int") * 30)
      .groupBy("video_id", "time_bucket")
      .count()
      .withColumnRenamed("count", "retention_score")
      .withColumnRenamed("video_id", "videoId")
      .orderBy(desc("retention_score"))
      .limit(100)

    println("--- 3. Feature: User Segmentation (K-Means) ---")
    val userFeatures = rawData
      .groupBy("user_id")
      .agg(
        avg(when($"event_type" === "STOP", $"timestamp_in_video" / $"video_duration_sec").otherwise(0)).alias("avg_completion_rate"),
        (sum(when($"event_type" === "SEEK_FORWARD", 1).otherwise(0)) / count("event_type")).alias("skip_ratio")
      )
      .na.fill(0)

    val assembler = new VectorAssembler()
      .setInputCols(Array("avg_completion_rate", "skip_ratio"))
      .setOutputCol("features")

    val featureVector = assembler.transform(userFeatures)

    val kmeans = new KMeans().setK(3).setSeed(1L)
    val model = kmeans.fit(featureVector)
    val predictions = model.transform(featureVector)

    val clusterResults = predictions.select("user_id", "prediction")
      .withColumnRenamed("prediction", "cluster_id")
      .withColumn("user_type", when($"cluster_id" === 0, "Casual Viewer")
        .when($"cluster_id" === 1, "Binge Watcher")
        .otherwise("Serial Skipper"))

    println("--- 4. Feature: AI Recommendations (ALS) ---")
    val userIndexer = new StringIndexer().setInputCol("user_id").setOutputCol("userId_idx")
    val videoIndexer = new StringIndexer().setInputCol("video_id").setOutputCol("videoId_idx")

    val indexerModelU = userIndexer.fit(rawData)
    val indexerModelV = videoIndexer.fit(rawData)

    val indexedData = indexerModelV.transform(indexerModelU.transform(rawData))

    val ratingData = indexedData
      .withColumn("completion_rate", $"timestamp_in_video" / $"video_duration_sec")
      .withColumn("rating",
        when($"completion_rate" > 0.5 || $"event_type" === "SEEK_BACKWARD", 1.0).otherwise(0.0)
      )
      .select($"userId_idx".cast("int"), $"videoId_idx".cast("int"), $"rating".cast("float"))

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId_idx")
      .setItemCol("videoId_idx")
      .setRatingCol("rating")

    val alsModel = als.fit(ratingData)
    val userRecs = alsModel.recommendForAllUsers(1)

    val recsExploded = userRecs
      .withColumn("rec", explode($"recommendations"))
      .select($"userId_idx", $"rec.videoId_idx".alias("recommended_vid_id"), $"rec.rating".alias("confidence"))

    // 5. EXPORT - Write CSV manually using Scala to avoid Hadoop issues on Windows
    println("Saving metrics to CSV...")
    val outputDir = "C:\\Users\\PC\\OneDrive\\Desktop\\VideoWatchProject\\output_metrics"
    new java.io.File(outputDir).mkdirs()

    // Helper function to write DataFrame to CSV using Scala (bypasses Hadoop)
    def dataframeToCSV(df: org.apache.spark.sql.DataFrame, path: String): Unit = {
      import java.io.PrintWriter
      val file = new java.io.File(path)
      file.getParentFile.mkdirs()
      val writer = new PrintWriter(file)
      try {
        // Write header
        writer.println(df.columns.mkString(","))
        // Write data rows
        df.collect().foreach { row =>
          writer.println(row.toSeq.map(_.toString).mkString(","))
        }
      } finally {
        writer.close()
      }
    }

    dataframeToCSV(videoMetrics, outputDir + "\\skip_data.csv")
    dataframeToCSV(boredomHeatmap, outputDir + "\\heatmap_data.csv")
    dataframeToCSV(safeZones, outputDir + "\\ad_spots.csv")
    dataframeToCSV(clusterResults, outputDir + "\\user_clusters.csv")
    dataframeToCSV(recsExploded, outputDir + "\\recommendations.csv")

    println("=================================================")
    println(">>> SPARK WEB UI IS NOW ACTIVE <<<")
    println(">>> Open in Browser: http://localhost:4040    <<<")
    println("=================================================")
    println("Press Ctrl+C to stop the application and close the UI (or send SIGTERM).")

    // Use a CountDownLatch so the process blocks even in non-interactive runs
    val latch = new CountDownLatch(1)

    // Add a JVM shutdown hook to release the latch when the process is being stopped
    sys.addShutdownHook {
      println("Shutdown hook triggered - stopping Spark...")
      try {
        spark.stop()
      } catch {
        case _: Throwable => // ignore shutdown errors
      }
      latch.countDown()
    }

    // Wait indefinitely until shutdown hook runs (e.g., Ctrl+C / service stop)
    try {
      latch.await()
    } catch {
      case e: InterruptedException => // ignore and exit
    }
  }
}
