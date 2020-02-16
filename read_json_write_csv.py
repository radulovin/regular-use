import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

def create_spark_session():
    spark = SparkSession \
        .builder \
        .appName("App Read json write csv") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_read_json(spark, input_data):
    #Load a json file into a spark dataframe called user_log
    user_log = spark.read.json(input_data)
    #Describe the schema:
    user_log.printSchema()
    #Describe returns the columns of the dataframe, just like printSchema:
    user_log.describe()
    #Check the records with show or take:
    user_log.show(n=1)
    user_log.take(5)
    return user_log

def process_write_csv(spark, input_dataframe, output_data):
    input_dataframe.write.save(output_data, format="csv", header=True)
    #Load the data from the csv file into a new dataframe user_log_2
    csv_df = spark.read.csv(out_path, header=True)
    csv_df.show(n=1)


def main():
    spark = create_spark_session()
    #Fetch all the parameters related to the Spark session
    spark.sparkContext.getConf().getAll()


    input_data_csv='s3a://emr-udacity-test/data.json'
    output_data_csv='s3a://emr-udacity-test/data.csv'


    df =  process_read_json(spark, input_data_csv)
    process_write_csv(spark, df, output_data_csv):


if __name__ == "__main__":
    main()
