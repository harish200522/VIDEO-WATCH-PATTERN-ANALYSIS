# Video Watch Project - Run Guide

## Running the Spark Application

### Prerequisites
- Java 21 (currently installed)
- sbt 1.11.7 (configured in `project/build.properties`)
- Dataset: `data/video_logs.csv` ✅ (included)

### How to Run

1. **Open PowerShell** and navigate to the project directory:
   ```powershell
   cd C:\Users\PC\OneDrive\Desktop\VideoWatchProject
   ```

2. **Start the application**:
   ```powershell
   sbt run
   ```

3. **Wait for startup**. You'll see messages like:
   ```
   [info] DEBUG: Loading from: C:/Users/PC/OneDrive/Desktop/VideoWatchProject/data/video_logs.csv
   [info] --- 1. Running Pattern Analysis ---
   [info] --- 2. Feature: Smart Ad-Insertion ---
   [info] --- 3. Feature: User Segmentation (K-Means) ---
   [info] --- 4. Feature: AI Recommendations (ALS) ---
   [info] Saving metrics to CSV...
   ```

4. **When you see the message**:
   ```
   [info] >>> SPARK WEB UI IS NOW ACTIVE <<<
   [info] >>> Open in Browser: http://localhost:4040
   ```
   The Spark Web UI is running!

### View the Spark Web UI

1. **Open a web browser** and navigate to:
   ```
   http://localhost:4040
   ```

2. **Explore the tabs**:
   - **Jobs**: Shows all Spark jobs running (analytics operations, clustering, recommendations)
   - **Stages**: Details on each stage of computation
   - **Executors**: Shows the local driver executor with memory/task info
   - **Storage**: RDD/DataFrame cache info
   - **SQL/DataFrame**: Query execution details

### Output Files

All results are saved to: `C:\Users\PC\OneDrive\Desktop\VideoWatchProject\output_metrics\`

- `skip_data.csv` - Video skip rate analysis
- `heatmap_data.csv` - Boredom heatmap (time buckets with skip counts)
- `ad_spots.csv` - Safe zones for ad insertion (retention scores)
- `user_clusters.csv` - User segmentation (KMeans clustering: Casual Viewer, Binge Watcher, Serial Skipper)
- `recommendations.csv` - AI recommendations (ALS model output)

### Stop the Application

Press **[ENTER]** in the PowerShell terminal to stop the app and close the Spark Web UI.

---

## Architecture

- **Backend**: Scala + Apache Spark (local[*] mode, all cores)
- **UI Binding**: `127.0.0.1:4040` (localhost)
- **Data Source**: `data/video_logs.csv` (CSV with user events: PLAY, SEEK_FORWARD, SEEK_BACKWARD, PAUSE, RESUME, STOP)
- **Analytics**:
  - Skip Rate: % of seeks per video
  - Boredom Heatmap: Time-based skip distribution
  - Safe Zones: Ad-insertion points (high retention)
  - User Segmentation: K-Means clustering (3 user types)
  - Recommendations: ALS collaborative filtering

