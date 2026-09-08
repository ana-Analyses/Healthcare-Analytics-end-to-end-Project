from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ✅ Step 1: Create schema
spark.sql("CREATE SCHEMA IF NOT EXISTS gold_layer")

# ✅ Step 2: Read silver (from correct Lakehouse)
silver_df = spark.read.table("LH_Silver.silver_layer.patient_enriched")

# -------------------------------
# Step 3: DIM PATIENT (SCD Type 2)
# -------------------------------

window_spec = Window.partitionBy("patient_id").orderBy(col("admission_time").desc())

silver_latest = (
    silver_df.withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

incoming_patient = silver_latest.select("patient_id", "gender", "age")

# Deterministic surrogate key
incoming_patient = incoming_patient.withColumn(
    "surrogate_key",
    abs(hash(col("patient_id")))
)
incoming_patient = (
    incoming_patient
    .withColumn("effective_from", current_timestamp())
    .withColumn("effective_to", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))
)

# Create table if not exists
if not spark. catalogue.tableExists("gold_layer.dim_patient"):
    incoming_patient.write.format("delta").mode("overwrite") \
        .saveAsTable("gold_layer.dim_patient")

# Load table
dim_patient = DeltaTable.forName(spark, "gold_layer.dim_patient")

# MERGE
dim_patient.alias("target").merge(
    incoming_patient.alias("source"),
    "target.patient_id = source.patient_id AND target.is_current = true"
).whenMatchedUpdate(
    condition="""
        target.gender <> source.gender OR 
        target.age <> source.age
    """,
    set={
        "is_current": "false",
        "effective_to": "source.effective_from"
    }
).whenNotMatchedInsert(
    values={
        "surrogate_key": "source.surrogate_key",
        "patient_id": "source.patient_id",
        "gender": "source.gender",
        "age": "source.age",
        "effective_from": "source.effective_from",
        "effective_to": "NULL",
        "is_current": "true"
    }
).execute()

# -------------------------------
# Step 4: DIM DEPARTMENT
# -------------------------------

dim_department_df = silver_df.select(
    "department", "hospital_id"
).dropDuplicates()

dim_department_df = dim_department_df.withColumn(
    "surrogate_key", monotonically_increasing_id()
)

dim_department_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_layer.dim_department")

# -------------------------------
# Step 5: FACT TABLE
# -------------------------------

dim_patient_df = spark.read.table("gold_layer.dim_patient") \
    .filter(col("is_current") == True) \
    .select(col("surrogate_key").alias("patient_sk"), "patient_id")

dim_dept_df = spark.read.table("gold_layer.dim_department") \
    .select(col("surrogate_key").alias("department_sk"), "department", "hospital_id")

fact_base = silver_df.select(
    "patient_id",
    "department",
    "hospital_id",
    "admission_time",
    "discharge_time",
    "bed_id"
)
fact_joined = fact_base \
    .join(dim_patient_df, on="patient_id", how="left") \
    .join(dim_dept_df, on=["department", "hospital_id"], how="left")

fact_final = fact_joined \
    .withColumn("fact_id", monotonically_increasing_id()) \
    .withColumn("admission_date", to_date("admission_time")) \
    .withColumn(
        "length_of_stay_hours",
        (unix_timestamp("discharge_time") - unix_timestamp("admission_time")) / 3600
    ) \
    .withColumn(
        "is_currently_admitted",
        when(col("discharge_time") > current_timestamp(), True).otherwise(False)
    ) \
    .withColumn("event_time", current_timestamp()) \
    .select(
        "fact_id",
        "patient_sk",
        "department_sk",
        "admission_time",
        "discharge_time",
        "admission_date",
        "length_of_stay_hours",
        "is_currently_admitted",
        "bed_id",
        "event_time"
    )

fact_final.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("admission_date") \
    .saveAsTable("gold_layer.fact_patient")

# -------------------------------
# Step 6: AGGREGATION TABLE
# -------------------------------

gold_summary = silver_df.groupBy(
    "hospital_id", "department"
).agg(
    count("*").alias("total_patients"),
    avg("age").alias("avg_age"),
    avg("stay_days").alias("avg_stay_days")
)

gold_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_layer.hospital_summary")

# -------------------------------
# Step 7: VALIDATION
# -------------------------------

print("Dim Patient:", spark.read.table("gold_layer.dim_patient").count())
print("Dim Department:", spark.read.table("gold_layer.dim_department").count())
print("Fact Table:", spark.read.table("gold_layer.fact_patient").count())









