import sys

from pyspark import SparkConf
from pyspark.sql import *

from lib.logger import Log4J
from lib.utils import *

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

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    logger.info("Starting HelloSpark")
    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())

    titanic_df = load_survey_df(spark, sys.argv[1])
    partitioned_titanic_df = titanic_df.repartition(2)
    print(titanic_df.count())
    count_df = count_by_dest(partitioned_titanic_df)
    
    logger.info(count_df.collect())

    #Pour que l'application ne s'arrête pas et que l'on puisse investiguer avec Spark UI
    input("Press enter")
    logger.info("Finished HelloSpark")
    spark.stop()
