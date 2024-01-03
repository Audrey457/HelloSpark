from pyspark import SparkConf
from pyspark.sql import *

from lib.logger import Log4J
from lib.utils import get_spark_app_config

if __name__ == "__main__":
    # En utilisant un fichier de config
    conf = get_spark_app_config()
    #harcodé
    # conf = SparkConf()
    # conf.set("spark.app.name", "Hello Spark")
    # conf.set("spark.master", "local[3]")
    spark = SparkSession.builder \
        .config(conf = conf) \
        .getOrCreate()
    # Équivalent
    # spark = SparkSession.builder \
    #     .appName("Hello Spark") \
    #     .master(("local[3]")) \
    #     .getOrCreate()

    logger = Log4J(spark)
    logger.info("Starting HelloSpark")
    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())
    logger.info("Finished HelloSpark")
    spark.stop()
