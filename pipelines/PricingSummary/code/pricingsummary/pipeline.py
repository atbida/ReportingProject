from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pricingsummary.config.ConfigStore import *
from pricingsummary.udfs.UDFs import *
from prophecy.utils import *
from pricingsummary.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Shipments = Shipments(spark)
    df_Cleanup = Cleanup(spark, df_Shipments)
    df_SumAmounts = SumAmounts(spark, df_Cleanup)
    df_ByStatus = ByStatus(spark, df_SumAmounts)
    Report(spark, df_ByStatus)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/PricingSummary")
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = spark.conf.get("prophecy.project.id") + "/" + "pipelines/PricingSummary"
    )
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
