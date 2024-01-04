import sys
from collections import namedtuple

from pyspark import SparkConf, SparkContext
from pyspark.sql import *

from lib.logger import Log4J
from lib.utils import *

TitanicRecord = namedtuple("TitanicRecord", ["name", "sex", "age", "dest"])
if __name__ == "__main__":
    conf = get_spark_app_config("RDD_APP_CONFIGS")
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloRDD <filename>")
        sys.exit(-1)

    logger.info("Starting HelloRDD")

    # Pour rdd : note : pas recommandé d'utiliser RDD
    sc = spark.sparkContext
    # créer RDD
    linesRDD = sc.textFile(sys.argv[1])
    partitionedRDD = linesRDD.repartition(2)
    # nettoyer rdd et créer colonnes
    colsRDD = partitionedRDD.map(lambda line: line.replace('"', '').split(";"))

    # select
    selectRDD = colsRDD.map(lambda cols: TitanicRecord(cols[0], cols[1], float(cols[2]) if cols[2] else 0, cols[3]))

    # filtrer
    filteredRDD = selectRDD.filter(lambda r: r.age < 40)

    # count
    kvRDD = filteredRDD.map(lambda r: (r.dest, 1))
    countRDD = kvRDD.reduceByKey(lambda v1, v2: v1 + v2)

    colsList = countRDD.collect()
    for x in colsList:
        logger.info(x)

    # Pour que l'application ne s'arrête pas et que l'on puisse investiguer avec Spark UI
    input("Press enter")
    logger.info("Finished HelloSpark")
    spark.stop()