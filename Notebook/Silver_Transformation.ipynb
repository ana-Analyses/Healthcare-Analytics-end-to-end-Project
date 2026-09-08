from pyspark.sql.functions import *

#  Step 1: Create schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS silver_layer")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver_quarantine")

#  Step 2: Read Bronze table
bronze_df = spark.read.table("LH_Bronze.Bronze_layer.patient_clean")

#  Step 3: Common transformations
df = bronze_df \
    .withColumn("admission_time", to_timestamp("admission_time")) \
    .withColumn("discharge_time", to_timestamp("discharge_time")) \
    .withColumn("ingestion_time", current_timestamp())

#  Step 4: VALID DATA (Silver Layer)
valid_df = df \
    .filter((col("age") > 10) & (col("age") <= 100)) \
    .filter(col("admission_time") <= current_timestamp()) \
    .filter(col("discharge_time") > col("admission_time")) \
    .withColumn("department", upper(col("department"))) \
    .withColumn("gender", initcap(col("gender"))) \
    .withColumn(
        "stay_hours",
        (col("discharge_time").cast("long") - col("admission_time").cast("long")) / 3600
    ) \
    .withColumn("stay_days", col("stay_hours") / 24) \
    .dropDuplicates(["patient_id"])

#  Step 5: INVALID DATA (Quarantine Layer)
invalid_df = df \
    .filter(
        (col("age") <= 10) |
        (col("age") > 100) |
        (col("admission_time") > current_timestamp()) |
        (col("discharge_time") <= col("admission_time"))
    ) \
    .withColumn(
        "error_reason",
        when(col("age") <= 0, "Invalid Age <= 0")
        .when(col("age") > 100, "Invalid Age > 100")
        .when(col("admission_time") > current_timestamp(), "Future Admission Time")
        .when(col("discharge_time") <= col("admission_time"), "Invalid Discharge Time")
        .otherwise("Unknown Error")
    ) \
    .withColumn("error_timestamp", current_timestamp())

#  Step 6: Write Silver table
valid_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("LH_silver.silver_layer.patient_enriched")

# Step 7: Write Quarantine table
invalid_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("LH_silver.silver_quarantine.patient_bad_records")
