from pyspark.sql.functions import *
from pyspark.sql.types import *

#  Step 1: Create schemas (if not exists)
spark.sql("CREATE SCHEMA IF NOT EXISTS LH_Bronze.Bronze_layer")
spark.sql("CREATE SCHEMA IF NOT EXISTS LH_Bronze.Bronze_quarantine")

#   Read RAW data from Lakehouse
raw_df = spark.sql("SELECT * FROM LH_Raw.Raw_schema.raw")

#  Common Transformations
df = raw_df \
    .withColumn("admission_time", to_timestamp("admission_time")) \
    .withColumn("discharge_time", to_timestamp("discharge_time")) \
    .withColumn("ingestion_time", current_timestamp())

#   VALID DATA (Bronze Layer)
valid_df = df.filter(
    (col("age") > 0) & (col("age") <= 100) &
    (col("admission_time") <= current_timestamp()) &
    (col("discharge_time") > col("admission_time"))
).dropDuplicates(["patient_id"])

#   INVALID DATA (Quarantine Layer)
invalid_df = df.filter(
    (col("age") > 100) |
    (col("age") <= 0) |
    (col("admission_time") > current_timestamp()) |
    (col("discharge_time") <= col("admission_time"))
)
#  Add Error Reason Column
invalid_df = invalid_df.withColumn(
    "error_reason",
    when(col("age") > 100, "Invalid Age > 100")
    .when(col("age") <= 0, "Invalid Age <= 0")
    .when(col("admission_time") > current_timestamp(), "Future Admission Time")
    .when(col("discharge_time") <= col("admission_time"), "Invalid Discharge Time")
    .otherwise("Unknown Error")
).withColumn("error_timestamp", current_timestamp())

#   Write Bronze Table (Valid Data)
valid_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("LH_Bronze.Bronze_layer.patient_clean")

#   Write Quarantine Table (Invalid Data)
invalid_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("LH_Bronze.Bronze_quarantine.patient_bad_records")

















