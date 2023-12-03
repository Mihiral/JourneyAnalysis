from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, substring
from pyspark.sql.functions import current_timestamp

def run_pyspark_job():
    # Initialize Spark session
    spark = SparkSession.builder.appName("S3ToAPI").getOrCreate()

    # Read data from AWS S3
    s3_path = "s3a://journeydata/event"
    df = spark.read.parquet(s3_path)

    # Perform transformations or filtering if needed

    def flatten_nested_columns(df):
        """
        Flatten nested columns in a PySpark DataFrame.

        Parameters:
        - df (DataFrame): Input DataFrame.

        Returns:
        - DataFrame: DataFrame with flattened columns.
        """
        # Identify all nested columns in the DataFrame
        nested_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, (ArrayType, MapType, StructType))]

        # Flatten each nested column
        for nested_column in nested_columns:
            # Get the schema of the nested column
            nested_schema = df.schema[nested_column].dataType

            # Get a list of all the nested fields
            nested_fields = [f.name for f in nested_schema.fields]

            # Create a list of exploded columns
            exploded_columns = [explode(col(f"{nested_column}.{field}")).alias(f"{nested_column}_{field}") for field in nested_fields]

            # Flatten the DataFrame
            df = df.select("*", *exploded_columns).drop(nested_column)

        return df


    flatteneddf = flatten_nested_columns(df)

    new_df = flatteneddf.withColumn("load_date", current_timestamp()) \
        .withColumn("id", substring(col("eventname"),10, -1)) \
        .withColumn("salary",col("salary")*100) \
        .withColumnRenamed("gender","sex")

    flatteneddf.write.format("parquet").mode("append").save(f"s3a://journey/event_load")


    # Stop the Spark session
    spark.stop()

