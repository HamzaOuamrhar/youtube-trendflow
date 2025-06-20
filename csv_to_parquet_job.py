import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, regexp_extract
from pyspark.sql.types import LongType, IntegerType

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['S3_INPUT_PATH', 'S3_OUTPUT_PATH'])
s3_input_path = args['S3_INPUT_PATH']
s3_output_path = args['S3_OUTPUT_PATH']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

allowed_regions = ['ca', 'us']
input_paths = [f"{s3_input_path}region={region}" for region in allowed_regions]

df = spark.read.option("header", "true").csv(input_paths)

df = df.withColumn(
    "region",
    regexp_extract(input_file_name(), r"region=([a-z]{2})", 1)
)

df = df.withColumn("category_id_int", col("category_id").cast(IntegerType()))
df = df.filter(col("category_id_int").isNotNull())
df = df.drop("category_id").withColumnRenamed("category_id_int", "category_id")

for field in df.schema.fields:
    if isinstance(field.dataType, (LongType, IntegerType)):
        df = df.withColumn(field.name, col(field.name).cast("bigint"))

df = df.repartition("region")

df.write \
    .mode("overwrite") \
    .partitionBy("region") \
    .option("maxRecordsPerFile", 999999999) \
    .parquet(s3_output_path)

spark.stop()
print("Job completed successfully.")
