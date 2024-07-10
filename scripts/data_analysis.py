from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, lag, when, year, month
from pyspark.sql.window import Window
import sys

def analyze_data(input_path):
    spark = SparkSession.builder.appName("DataAnalysis").getOrCreate()

    # Load data
    df = spark.read.format("delta").load(input_path)
    # df.display()

    # Moving Averages (7-day and 30-day)
    window_spec_7 = Window.partitionBy("ticker").orderBy("date").rowsBetween(-6, 0)
    window_spec_30 = Window.partitionBy("ticker").orderBy("date").rowsBetween(-29, 0)
    moving_averages = df.withColumn("ma_7", avg("close").over(window_spec_7)).withColumn("ma_30", avg("close").over(window_spec_30))
    moving_averages.select("ticker", "date", "ma_7", "ma_30").display()

    # Percentage Change
    window_spec = Window.partitionBy("ticker").orderBy("date")
    percentage_change = df.withColumn("prev_close", lag("close").over(window_spec)).withColumn("pct_change", (col("close") - col("prev_close")) / col("prev_close") * 100)
    percentage_change.select("ticker", "date", "pct_change").filter(col("pct_change").isNotNull()).display()

     # Monthly Aggregates
    df = df.withColumn("year", year("date")).withColumn("month", month("date"))
    monthly_aggregates = df.groupBy("ticker", "year", "month").agg(
        avg("close").alias("monthly_avg_close"),
        max("close").alias("monthly_max_close"),
        min("close").alias("monthly_min_close")
    )
    monthly_aggregates.display()

    # Yearly Trends
    yearly_trends = df.groupBy("ticker", "year").agg(avg("close").alias("yearly_avg_close"))
    yearly_trends.display()
    

input_path = sys.argv[1]


# input_path = '../data/standard_raw/'

analyze_data(input_path)