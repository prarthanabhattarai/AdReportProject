from pyspark.sql import SQLContext
from pyspark import SparkContext
from cassandra.cluster import Cluster
from pyspark import SparkConf
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table
from set_up_table import bid_table 

#Connect to the demo keyspace on our cluster running at 127.0.0.1
connection.setup(['127.0.0.1'], "fb_report")

#Create a sql context
sc = SparkContext("spark://ip-172-31-9-43:7077", "fb_report")
sqlContext = SQLContext(sc)

#read json data from hdfs
path = "hdfs://ec2-52-8-165-110.us-west-1.compute.amazonaws.com:9000/user/AdReport/fb_bids/history"
ad_camps = sqlContext.jsonFile(path)

ad_camps.printSchema()
print (type(bid_table))

# Register this SchemaRDD as a table.
ad_camps.registerTempTable("ad_camps")

# SQL statements can be run by using the sql methods provided by sqlContext.
bid_infos = sqlContext.sql("SELECT account_id, bid_info.ACTIONS, bid_info.CLICKS, bid_info.IMPRESSIONS, bid_info.REACH, bid_info.SOCIAL, bid_type, targeting.age_max, targeting.age_min, targeting.genders[0], targeting.geo_locations.countries[0]  FROM ad_camps").collect()
bid_infoRDD = sc.parallelize(bid_infos)

def makeTable(list):
	bid_table.create(ad_group_id=list[0], bid_actions=list[1], bid_clicks=list[2], bid_impressions=list[3],bid_reach=list[4],bid_social=list[5], bid_type=list[6], target_max_age=list[7], target_min_age=list[8], target_gender=list[9],target_country=list[10])

bid_RDD=bid_infoRDD.foreach(lambda x: makeTable(x))

