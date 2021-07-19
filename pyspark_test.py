import findspark
findspark.init()
findspark.add_packages('mysql:mysql-connector-java:8.0.25')

import py4j
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.config('C:/spark/spark-3.1.2-bin-hadoop3.2/jars/spark.jars', 'C:/mysql-connector-java-8.0.25/mysql-connector-java-8.0.25.jar') \
    .master('local').appName('PySpark_MySQL_test').getOrCreate()
sql_url = 'localhost'
user = 'root'
password = 'password'
database = 'labeling_direction'
table = 'direction_folder'
df = spark.read.format('jdbc')\
    .option('driver', 'com.mysql.jdbc.Driver')\
    .option('url', 'jdbc:mysql://{}:3306/{}? '.format(sql_url, database))\
    .option('user', user)\
    .option('password', password)\
    .option('dbtable', table)\
    .load()
df.show()
