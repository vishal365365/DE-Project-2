import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import StructField, StructType, IntegerType, StringType
from pyspark.sql.functions import year,to_date
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME","file_path","bucket_name"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
rating_schema = StructType([StructField("userId",IntegerType(),False),StructField("movieId",IntegerType(),False),\
         StructField("rating",IntegerType(),False),StructField("timestamp",StringType(),False)])
movie_schema = StructType([StructField("id",IntegerType(),False),StructField("title",StringType(),False),\
               StructField("genres",StringType(),False)])
# Script generated for node Amazon S3
movie = spark.read.format("csv").options(header = "false").schema(movie_schema).load("s3://etl-movie-datas/movie_list/movie.csv")
rating = spark.read.format("csv").options(header = "true",recursiveFileLookup = "true" ).schema(rating_schema).load(f"s3://{args['bucket_name']}/{args['file_path']}")
movie_rating = rating.join(movie,movie["id"] == rating["movieId"],"inner")
movie_rating = movie_rating.select("movieId","title","rating","timestamp",year("timestamp").alias('year'),to_date("timestamp").alias("date"))
movie_rating.write.format("parquet").mode("append").partitionBy("year","date").save("s3://etl-movie-datas/movie_list/movie_data_filtered/")
job.commit()
