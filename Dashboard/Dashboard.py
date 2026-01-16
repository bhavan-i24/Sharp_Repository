#schema

from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, ArrayType
)
usage_metric_schema = StructType([
    StructField("color", LongType(), True),
    StructField("monochrome", LongType(), True),
    StructField("value", LongType(), True)
])
output_detail_schema = StructType([
    StructField("copies", usage_metric_schema, True),
    StructField("fax", usage_metric_schema, True),
    StructField("filingData", usage_metric_schema, True),
    StructField("internetFax", usage_metric_schema, True),
    StructField("others", usage_metric_schema, True),
    StructField("prints", usage_metric_schema, True)
])
send_detail_schema = StructType([
    StructField("faxSend", usage_metric_schema, True),
    StructField("internetFaxSend", usage_metric_schema, True),
    StructField("scanToEmail", usage_metric_schema, True),
    StructField("scanToHDD", usage_metric_schema, True)
])
daily_usage_schema = StructType([
    StructField(
        "detail",
        StructType([
            StructField("output", output_detail_schema, True),
            StructField("send", send_detail_schema, True)
        ]),
        True
    ),
    StructField(
        "total",
        StructType([
            StructField("output", usage_metric_schema, True),
            StructField("send", usage_metric_schema, True)
        ]),
        True
    )
])
schema_counter  = StructType([
    StructField("L1DealerId", StringType(), True),
    StructField("L2DealerId", StringType(), True),
    StructField("L3DealerId", StringType(), True),

    StructField("_id", StructType([
        StructField("$oid", StringType(), True)
    ]), True),

    StructField("dailyUsage", daily_usage_schema, True),

    StructField("deviceId", StringType(), True),
    StructField("divisionId", StringType(), True),
    StructField("familyName", StringType(), True),
    StructField("friendlyName", StringType(), True),
    StructField("ipAddress", StringType(), True),
    StructField("ipToLong", StringType(), True),
    StructField("location", StringType(), True),
    StructField("macAddress", StringType(), True),
    StructField("machineId", StringType(), True),
    StructField("modelName", StringType(), True),
    StructField("pk", StringType(), True),
    StructField("registeredTime", StringType(), True),
    StructField("relatedAgentId", StringType(), True),
    StructField("serialNumber", StringType(), True),
    StructField("servicingDealerId", StringType(), True),

    StructField("tagIds", ArrayType(StringType(), True), True),

    StructField("timestamp", StructType([
        StructField("$date", StringType(), True)
    ]), True)
])
#Reading the file 
counter_data = (
    spark.read
    .option("inferSchema", "false")
    .schema(schema_counter)
    .option("multiLine", "true")
    .json("/Volumes/workspace/default/json_raw_volume/counterUsage.json")
)
counter_data.printSchema()

#creating delta table 

%sql
CREATE TABLE IF NOT EXISTS Counter_DailyUsage (
    L1DealerId STRING,
    L2DealerId STRING,
    L3DealerId STRING,
    id STRING,

    deviceId STRING,
    divisionId STRING,
    familyName STRING,
    friendlyName STRING,
    ipAddress STRING,
    ipToLong STRING,
    location STRING,
    macAddress STRING,
    machineId STRING,
    modelName STRING,
    pk STRING,
    registeredTime STRING,
    relatedAgentId STRING,
    serialNumber STRING,
    servicingDealerId STRING,

    tagIds ARRAY<STRING>,

    usage_detail_output STRUCT<
        copies:STRUCT<color:BIGINT, monochrome:BIGINT, value:BIGINT>,
        fax:STRUCT<color:BIGINT, monochrome:BIGINT, value:BIGINT>,
        filingData:STRUCT<color:BIGINT, monochrome:BIGINT, value:BIGINT>,
        internetFax:STRUCT<color:BIGINT, monochrome:BIGINT, value:BIGINT>,
        others:STRUCT<color:BIGINT, monochrome:BIGINT, value:BIGINT>,
        prints:STRUCT<color:BIGINT, monochrome:BIGINT, value:BIGINT>
    >,

    usage_detail_send STRUCT<
        faxSend:STRUCT<color:BIGINT, monochrome:BIGINT, value:BIGINT>,
        internetFaxSend:STRUCT<color:BIGINT, monochrome:BIGINT, value:BIGINT>,
        scanToEmail:STRUCT<color:BIGINT, monochrome:BIGINT, value:BIGINT>,
        scanToHDD:STRUCT<color:BIGINT, monochrome:BIGINT, value:BIGINT>
    >,

    usage_total_output STRUCT<color:BIGINT, monochrome:BIGINT, value:BIGINT>,
    usage_total_send STRUCT<color:BIGINT, monochrome:BIGINT, value:BIGINT>,

    usage_timestamp TIMESTAMP
)
USING DELTA;

#table ready formatting 

from pyspark.sql.functions import col, to_timestamp
final_counter = counter_data.select(
    col("L1DealerId"),
    col("L2DealerId"),
    col("L3DealerId"),
    col("_id.$oid").alias("id"),

    col("deviceId"),
    col("divisionId"),
    col("familyName"),
    col("friendlyName"),
    col("ipAddress"),
    col("ipToLong"),
    col("location"),
    col("macAddress"),
    col("machineId"),
    col("modelName"),
    col("pk"),
    col("registeredTime"),
    col("relatedAgentId"),
    col("serialNumber"),
    col("servicingDealerId"),

    col("tagIds"),

    col("dailyUsage.detail.output").alias("usage_detail_output"),
    col("dailyUsage.detail.send").alias("usage_detail_send"),

    col("dailyUsage.total.output").alias("usage_total_output"),
    col("dailyUsage.total.send").alias("usage_total_send"),

    to_timestamp(col("timestamp.$date")).alias("usage_timestamp")
)
final_counter.printSchema()

#inserting into table 

final_counter.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("Counter_DailyUsage")

#dashboard ready transformation 

from pyspark.sql.functions import (
    col, to_date, date_trunc, coalesce, lit
)
usage_base = spark.table("Counter_DailyUsage").select(
    col("pk").alias("customer_id"),      # ✅ CUSTOMER
    col("deviceId"),

    # optional metadata (safe to keep)
    col("friendlyName"),
    col("location"),

    col("usage_timestamp"),

    col("usage_total_output.value").alias("output_count"),
    col("usage_total_send.value").alias("send_count")
)
usage_base.display()
usage_clean = (
    usage_base
    .withColumn("output_count", coalesce(col("output_count"), lit(0)))
    .withColumn("send_count", coalesce(col("send_count"), lit(0)))
)
usage_clean.display()
usage_enriched = (
    usage_clean
    .withColumn("usage_date", to_date("usage_timestamp"))
    .withColumn("usage_month", date_trunc("month", col("usage_timestamp")))
    .withColumn(
        "total_usage",
        col("output_count") + col("send_count")
    )
)
usage_enriched.display()
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
window_dedup = Window.partitionBy(
    "customer_id", "deviceId", "usage_date"
).orderBy(col("usage_timestamp").desc())
usage_dedup = (
    usage_enriched
    .withColumn("rn", row_number().over(window_dedup))
    .filter(col("rn") == 1)
    .drop("rn")
)
usage_dedup.display()
from pyspark.sql.functions import col, when, trim

usage_normalized = (
    usage_dedup
    .withColumn(
        "friendlyName",
        when(
            trim(col("friendlyName")).isin("", "null", "NULL", "N/A"),
            None
        ).otherwise(col("friendlyName"))
    )
    .withColumn(
        "location",
        when(
            trim(col("location")).isin("", "null", "NULL", "N/A"),
            None
        ).otherwise(col("location"))
    )
)
usage_normalized.display()
from pyspark.sql.functions import coalesce, lit
final_fill = (
    usage_normalized
    .withColumn("friendlyName", coalesce(col("friendlyName"), lit("Unknown")))
    .withColumn("location", coalesce(col("location"), lit("Unknown")))
)
final_fill.display()

final_fill.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("Counter_Dashboard_Usage")


