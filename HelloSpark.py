import sys
from collections import namedtuple

from pyspark import SparkConf, SparkContext
from pyspark.sql import *

from lib.logger import Log4J
from lib.utils import *

TitanicRecord = namedtuple("TitanicRecord", ["name", "sex", "age", "dest"])
if __name__ == "__main__":
    # En utilisant un fichier de config
    conf = get_spark_app_config("SPARK_APP_CONFIGS")
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

    #Dataframe
    titanic_df = load_titanic_df(spark, sys.argv[1])
    partitioned_titanic_df = titanic_df.repartition(2)
    print(titanic_df.count())
    count_df = count_by_dest(partitioned_titanic_df)

    logger.info(count_df.collect())
