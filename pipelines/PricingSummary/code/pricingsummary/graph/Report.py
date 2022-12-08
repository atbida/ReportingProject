from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pricingsummary.config.ConfigStore import *
from pricingsummary.udfs.UDFs import *

def Report(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("overwriteSchema", True)\
        .mode("overwrite")\
        .saveAsTable(f"test_delta.report_shipping_pricing")
