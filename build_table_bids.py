from pyspark.sql import SQLContext
from pyspark import SparkContext
from cassandra.cluster import Cluster
from pyspark import SparkConf
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table

class bid_table(Model):

        account_id = columns.BigInt(primary_key = True)
        ad_group_id = columns.BigInt(primary_key = True)
	bid_actions = columns.Float()
        bid_clicks = columns.Float()
        bid_impressions = columns.Float()
        bid_reach = columns.Float()
        bid_social = columns.Float()
        bid_type = columns.Text()
        target_max_age = columns.Integer()
        target_min_age = columns.Integer()
        target_gender = columns.Integer()
        target_country = columns.Text()

# Connect to the demo keyspace on our cluster running at 127.0.0.1
connection.setup(['127.0.0.1'], "fb_report")

#Create a sql context
sc = SparkContext("spark://ip-172-31-9-43:7077", "fb_report")
sqlContext = SQLContext(sc)

#read json data from hdfs
path = "hdfs://ec2-52-8-165-110.us-west-1.compute.amazonaws.com:9000/user/AdReport/fb_bids/history"
ad_camps = sqlContext.jsonFile(path)

#ad_camps.printSchema()
#Sync your model with your cql table
sync_table(bid_table)

# Register this SchemaRDD as a table.
ad_camps.registerTempTable("ad_camps")

#SQL statements can be run by using the sql methods provided by sqlContext.
bid_infos = sqlContext.sql("SELECT account_id, id, bid_info.ACTIONS, bid_info.CLICKS, bid_info.IMPRESSIONS, bid_info.REACH, bid_info.SOCIAL,bid_type, targeting.age_max, targeting.age_min, targeting.genders[0], targeting.geo_locations.countries[0]  FROM ad_camps").collect()

#Results of SQL queries are RDDs, RDD methods can be applied to them
bidRDD = sc.parallelize(bid_infos)

print (bidRDD.count())

for info in bid_infos:
	bid_table.create(account_id=info[0], ad_group_id=info[1],bid_actions= info[2],bid_clicks=info[3],bid_impressions= info[4],bid_reach=info[5],bid_social=info[6], bid_type=info[7], target_max_age=info[8], target_min_age=info[9], target_gender=info[10],target_country=info[11])
