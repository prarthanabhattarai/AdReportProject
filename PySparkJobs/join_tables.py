from pyspark.sql import SQLContext
from pyspark import SparkContext
from cassandra.cluster import Cluster
from pyspark import SparkConf
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table
import pandas as pd

class joined_table(Model):
	account_id = columns.BigInt(primary_key = True)
        ad_group_id = columns.BigInt(primary_key = True)
        bid_type = columns.Text()
	clicks = columns.Float()
        cost_per_unique_click = columns.Float()
	cost_per_result = columns.Float()
	reach = columns.Integer()
	result_rate = columns.Float()
	date_start = columns.Text()

#Connect to the demo keyspace on our cluster running at 127.0.0.1
connection.setup(['127.0.0.1'], "fb_report")

#Create a sql context
sc = SparkContext("spark://ip-172-31-9-43:7077", "fb_report")
sqlContext = SQLContext(sc)

#read json data from hdfs
path1 = "hdfs://ec2-52-8-165-110.us-west-1.compute.amazonaws.com:9000/user/AdReport/fb_bids/history"
ad_camp_bid = sqlContext.jsonFile(path1)

path2 = "hdfs://ec2-52-8-165-110.us-west-1.compute.amazonaws.com:9000/user/AdReport/ads_info/history"
ad_camp_stats = sqlContext.jsonFile(path2)

ad_camp_bid.printSchema()
#ad_camp_stats.printSchema()

sync_table(joined_table)

# Register this SchemaRDD as a table.
ad_camp_bid.registerTempTable("ad_camp_bid")
ad_camp_stats.registerTempTable("ad_camp_stats")

# SQL statements can be run by using the sql methods provided by sqlContext.
#info1 = sqlContext.sql("SELECT account_id, bid_type, targeting.age_max, targeting.age_min  FROM ad_camp_bid").collect()
#info2 = sqlContext.sql("SELECT campaign_id,actions_per_impression,clicks,cost_per_unique_click,date_start,date_stop FROM ad_camp_stats").collect()
info3 = sqlContext.sql("SELECT ad_camp_bid.account_id, ad_camp_bid.id, ad_camp_bid.bid_type, ad_camp_stats.clicks, ad_camp_stats.cost_per_unique_click,ad_camp_stats.cost_per_result,ad_camp_stats.reach,ad_camp_stats.result_rate,ad_camp_stats.date_start FROM ad_camp_bid INNER JOIN ad_camp_stats ON ad_camp_bid.id = ad_camp_stats.campaign_id").collect()
#info4 = sqlContext.sql("SELECT bid_type, COUNT(bid_type) FROM ad_camp_bid GROUP BY bid_type").collect()
#infodf = pd.DataFrame(info4)

#print (infodf.head(10))
for info in info3:
        joined_table.create(account_id=info[0], ad_group_id=info[1],bid_type=info[2],clicks=info[3],cost_per_unique_click=info[4],cost_per_result=info[5],reach=info[6], result_rate=info[7],date_start=info[8])
print ("finished creating table: joined table")
