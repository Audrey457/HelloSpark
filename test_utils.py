import warnings
from unittest import TestCase
from pyspark.sql import SparkSession
from lib.utils import *

class UtilsTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("HelloSparkTest") \
            .getOrCreate()

    def test_datafile_loading(self):
        sample_df = load_titanic_df(self.spark, "data/titanic.csv")
        result_count = sample_df.count()
        self.assertEqual(result_count, 1309, "Record count should be 1309")

    def test_country_count(self):
        sample_df = load_titanic_df(self.spark, "data/titanic.csv")
        count_list = count_by_dest(sample_df).collect()
        count_dict = dict()
        for row in count_list:
            count_dict[row["dest"]] = row["count"]
        self.assertEqual(count_dict["New York, NY"], 29, "Count for NY should be 29")
        self.assertEqual(count_dict["Cornwall / Akron, OH"], 8, "Count for Cornwall / Akron, OH should be 8")
        self.assertEqual(count_dict["Sweden Winnipeg, MN"], 7, "Count for Sweden Winnipeg, MN should be 7")



    @classmethod
    def tearDownClass(cls)->None:
        cls.spark.stop()