import sys
from pyspark.sql import SparkSession

class DataLoader:
    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path
        self.df = None
        self.spark = SparkSession.builder.appName("ReadFiles").getOrCreate()

    def read_csv_files(self, file_name):
        self.df = self.spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(self.input_path + "*.csv")
        self.process_and_save(self.df, file_name)

    def process_and_save(self, df, file_name):
        self.df.write.format("delta").mode("overwrite").partitionBy("ticker").save(self.output_path)



input_path = sys.argv[1]
output_path = sys.argv[2]

# input_path = '../data/raw/'
# output_path = '../data/standard_raw/'


loader = DataLoader(input_path, output_path)
loader.read_csv_files("crypto_prices")
