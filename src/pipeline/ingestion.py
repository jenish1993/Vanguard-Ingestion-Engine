import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

class PySparkIngestionPipeline:
    def __init__(self, schema, app_name="IngestionPipeline"):
        self.app_name = app_name
        self.spark = None
        self.schema = schema

    def __enter__(self):
        logger.info(f"Initializing SparkSession: {self.app_name}")
        self.spark = SparkSession.builder \
            .appName(self.app_name) \
            .master("local[*]") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
            .getOrCreate()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.spark:
            logger.info("Stopping SparkSession and releasing JVM resources.")
            self.spark.stop()

    def extract(self, file_path):
        logger.info(f"Extracting data from: {file_path}")
        return self.spark.read.csv(
            file_path,
            schema=self.schema,
            header=True,
            inferSchema=False
        )

    def transform(self, df):
        logger.info("Transforming data (Identity block).")
        return df

    def load(self, df, db_url, table_name, mode="append", properties=None):
        logger.info(f"Loading data into table: {table_name} at {db_url}")
        if properties is None:
            properties = {}
        df.write.jdbc(url=db_url, table=table_name, mode=mode, properties=properties)
