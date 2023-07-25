# Databricks notebook source
#creating mount point
dbutils.fs.mount(source = 'wasbs://adfcontainer@manidb1azure.blob.core.windows.net',
mount_point = '/mnt/adf_assignment',
extra_configs = {'fs.azure.account.key.manidb1azure.blob.core.windows.net':'X96T9Vc26NOxHA2ZnMomDDDLtPhUeGwNXub8cFQ1cOZgSDJNk5yHdKE/h1hIZjtawWRb2gozVuCH+AStZlT6sw=='}
)

# COMMAND ----------

# reading the open api file json
import requests

response=requests.get('https://api.publicapis.org/entries')
animal_rd =spark.sparkContext.parallelize([response.text])

df=spark.read.option("multiline",True).json(animal_rd)
display(df)

# COMMAND ----------

df.write.json('/mnt/adf_assignment/bronze/animals.json')

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql.functions import explode

schema = StructType([
    StructField("count",IntegerType(), True),
    StructField("entries",ArrayType(StructType([
        StructField("API", StringType(), True),
        StructField("Auth", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Cors", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("HTTPS", StringType(), True),
        StructField("Link", StringType(), True)])), True)])
jdf = spark.read.json("/mnt/adf_assignment/bronze/animals.json", schema=schema)
exploded_df = jdf.select("count", explode("entries").alias("data"))
display(exploded_df)

exploded_df.write.json('/mnt/adf_assignment/bronze/animals_jdf.json')

# COMMAND ----------

#reading the data from bronze layer
adf_df = spark.read.option('header', True).json('/mnt/adf_assignment/bronze/animals_jdf.json')
display(adf_df)

# COMMAND ----------

