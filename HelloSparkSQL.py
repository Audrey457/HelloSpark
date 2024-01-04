import sys
from collections import namedtuple

from pyspark import SparkConf, SparkContext
from pyspark.sql import *

from lib.logger import Log4J
from lib.utils import *

if __name__ == "__main__":
    # En utilisant un fichier de config
    conf = get_spark_app_config("SPARKSQL_APP_CONFIGS")
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSparkSQL <filename>")
        sys.exit(-1)

    titanic_df = load_titanic_df(spark, sys.argv[1])

    titanic_df.withColumnRenamed("home.dest", "dest").createOrReplaceTempView("titanic_tbl")
    count_df = spark.sql("SELECT dest, count(1) as count from titanic_tbl where age < 40 group by dest")
    count_df.show()