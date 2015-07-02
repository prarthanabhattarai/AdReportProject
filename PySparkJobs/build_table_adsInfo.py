from pyspark.sql import SQLContext
from pyspark import SparkContext
from cassandra.cluster import Cluster
from pyspark import SparkConf
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table
import pandas as pd

class adsinfo(Model):

        ad_id = columns.BigInt(primary_key = True)
        actions_per_impression = columns.Float()
        clicks = columns.Float()
        cost_per_unique_click = columns.Float()
        cost_per_result = columns.Float()
	result_rate = columns.Float()
	reach = columns.Integer()
        date_start = columns.Text(primary_key = True)
        date_stop = columns.Text()

# Connect to the demo keyspace on our cluster running at 127.0.0.1
connection.setup(['127.0.0.1'], "fb_report")

#Create a sql context
sc = SparkContext("spark://ip-172-31-9-43:7077", "fb_report")
sqlContext = SQLContext(sc)

#read json data from hdfs
path = "hdfs://ec2-52-8-165-110.us-west-1.compute.amazonaws.com:9000/user/AdReport/ads_info/history"
ad_camps = sqlContext.jsonFile(path)

#Sync your model with your cql table
sync_table(adsinfo)

# Register this SchemaRDD as a table.
ad_camps.registerTempTable("ad_camps")

#ad_camps.printSchema()
#SQL statements can be run by using the sql methods provided by sqlContext.
ads_infos = sqlContext.sql("SELECT campaign_id,actions_per_impression,clicks,cost_per_unique_click,cost_per_result,result_rate,reach,date_start,date_stop FROM ad_camps").collect()

#Results of SQL queries are RDDs, RDD methods can be applied to them
#adRDD = sc.parallelize(ads_infos)
#print (adRDD.take(10))

#addf=pd.DataFrame(adRDD)
#print (addf.head(10))
#Send table to Cassandra
for info in ads_infos:
	adsinfo.create(ad_id=info[0],actions_per_impression = info[1],clicks= info[2],cost_per_unique_click=info[3],cost_per_result=info[4],result_rate=info[5],reach=info[6],date_start=info[7], date_stop=info[8])

print ("finished creating table")
