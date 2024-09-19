# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

raw_folder_path = "/mnt/datalformulaone/raw"
processed_folder_path = "/mnt/datalformulaone/processed"
presentation_folder_path = "/mnt/datalformulaone/presentation"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

# COMMAND ----------

driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality", "team") \
.agg(sum("points").alias("total_points"),
    count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(driver_standings_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

#spark.sql("DROP TABLE IF EXISTS f1_presentation.driver_standings")
#final_df.write.format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------

#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------

overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.driver_standings;