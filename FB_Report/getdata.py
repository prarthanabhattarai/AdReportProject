from datetime import datetime, timedelta
from facebookads.api import FacebookAdsApi
import json, pprint, csv
from facebookads import objects
from facebookads.exceptions import FacebookRequestError
import yaml

 
class apiReader():

		def __init__(self, my_access_token, my_account_id, my_app_id = 'XXXXXXX', my_app_secret='XXXXXXXXXX'):
				self.my_app_id = my_app_id
				self.my_app_secret = my_app_secret
				self.my_access_token = my_access_token
				self.my_account_id = my_account_id
				FacebookAdsApi.init(self.my_app_id, self.my_app_secret, self.my_access_token)
				self.api = FacebookAdsApi.get_default_api()


		def get_bidding_info(self):

				#set params for getting ad sets from active campaigns
				params = {'fields':['account_id','campaign_status','bid_info','bid_type','targeting','start_time', 'end_time']}
				path = 'https://graph.facebook.com/v2.3/act_%s/adcampaigns?campaign_status=["ACTIVE"]&limit=500' % (self.my_account_id)

				try:
					response = self.api.call('GET',path, params = params)
					stats = response.json()
				        my_bidding_list=json.loads(json.dumps(stats['data']))
					
					#pprint.pprint (my_bidding_list)

					return my_bidding_list

				except (FacebookRequestError,AttributeError) as error:
					list=[]
					print (error)
					return list

		def get_ad_info(self, adset):

				#set params for getting info about ad sets
				#for daily update, set date_preset to last day
				params = {  'date_preset' :'last_28_days',	
						'time_increment' : '1'					
				}	
				path = 'https://graph.facebook.com/v2.3/%s/insights?limit=500' % (adset)
				response = self.api.call('GET', path, params = params)
					
				stats = response.json()	
				my_ads_list = json.loads(json.dumps(stats['data']))
				
				#print (adset)
				#pprint.pprint (my_ads_list)

				return my_ads_list



