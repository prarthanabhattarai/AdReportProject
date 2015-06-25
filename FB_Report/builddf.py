# sc is an existing SparkContext.
from pyspark.sql import SQLContext
from pyspark import SparkContext
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table

sc = SparkContext("spark://ip-172-31-1-201:7077", "fb_reports")
sqlContext = SQLContext(sc)

# A JSON dataset is pointed to by path.
# The path can be either a single text file or a directory storing text files.
#path = "/home/ubuntu/JSONfiles/hdfs_movietweetstest4_20150612185132.json"
#path = "hdfs://ec2-52-8-188-43.us-west-1.compute.amazonaws.com:9000/user/AdReport/history"
path = "hdfs://ec2-52-8-188-43.us-west-1.compute.amazonaws.com:9000/user/data.json"
# Create a SchemaRDD from the file(s) pointed to by path
ad_camps = sqlContext.jsonFile(path)

#user_table = ad_camps.cassandraTable("fb_reports", "bid_table")
#this is for connection to cassandra
connection.setup(['127.0.0.1'], "fb_reports")

# The inferred schema can be visualized using the printSchema() method.
ad_camps.printSchema()
# root
#  |-- age: IntegerType
#  |-- name: StringType

# Register this SchemaRDD as a table.
#ad_camps.registerTempTable("ad_camps")

df = ad_camps
df.show()

# SQL statements can be run by using the sql methods provided by sqlContext.
#teenagers = sqlContext.sql("SELECT text FROM people")

