# sc is an existing SparkContext.
from pyspark.sql import SQLContext
from pyspark import SparkContext
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table
import json 

sc = SparkContext("spark://ip-172-31-9-43:7077", "fb_reports")
sqlContext = SQLContext(sc)

path = "hdfs://ec2-52-8-165-110.us-west-1.compute.amazonaws.com:9000/user/AdReport/history"
#path = "hdfs://ec2-52-8-165-110.us-west-1.compute.amazonaws.com:9000/user/data.json"
# Create a SchemaRDD from the file(s) pointed to by path
ad_camps = sqlContext.jsonFile(path)

#ad_camps.printSchema()

print (type(ad_camps))
#ds = [json.dumps(item) for item in ad_camps]
#df = sqlContext.jsonRDD(sc.parallelize([json.dumps(ad_camps)]))

#user_table = ad_camps.cassandraTable("fb_reports", "bid_table")
#this is for connection to cassandra
#connection.setup(['127.0.0.1'], "fb_reports")

# The inferred schema can be visualized using the printSchema() method.
#print ("Showing Schema: ")
#df.printSchema()

#df.show()
# SQL statements can be run by using the sql methods provided by sqlContext.
#teenagers = sqlContext.sql("SELECT text FROM people")
