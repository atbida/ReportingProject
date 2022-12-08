from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pricingsummary.config.ConfigStore import *
from pricingsummary.udfs.UDFs import *

def Shipments(spark: SparkSession) -> DataFrame:
    from pyspark.dbutils import DBUtils

    return spark.read\
        .format("snowflake")\
        .options(
          **{
            "sfUrl": "https://lzcxmtj-mn69040.snowflakecomputing.com",
            "sfUser": DBUtils(spark).secrets.get(scope = "anyademos", key = "username"),
            "sfPassword": DBUtils(spark).secrets.get(scope = "anyademos", key = "password"),
            "sfDatabase": "RETAIL",
            "sfSchema": "PRICING",
            "sfWarehouse": "TPC"
          }
        )\
        .option("dbtable", "ORDERSHIPMENTS")\
        .load()
