from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pricingsummary.config.ConfigStore import *
from pricingsummary.udfs.UDFs import *

def ByStatus(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(col("RETURNFLAG").asc(), col("DELIVERYSTATUS").asc())
