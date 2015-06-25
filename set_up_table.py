from pyspark.sql import SQLContext
from pyspark import SparkContext
from cassandra.cluster import Cluster
from pyspark import SparkConf
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table

class bid_table(Model):

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

	sync_table(bid_table)
