from datetime import datetime, timedelta
from facebookads.api import FacebookAdsApi
import json, pprint, csv
from facebookads import objects
from facebookads.exceptions import FacebookRequestError
import yaml

#with open('names.txt') as f:
#    content = f.readlines()

#app_id = 'content[0]'
#app_secret='content[1]'
 
class apiReader():

		def __init__(self, my_access_token, my_account_id, my_app_id = 'XXXXXXXX', my_app_secret='XXXXXXXXX'):
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
				        my_bidding_list=json.loads(json.dumps(stats))
					
					#pprint.pprint (my_bidding_list)

					return my_bidding_list

					#if (len (my_bidding_list))>0:
					#	for i in range(len(my_bidding_list)):
					#		self.active_campaings.append(my_bidding_list['data'][i]['id'])
					#		print (self.active_campaings)

					#	with open('bidding_data.json', 'a') as outfile:
					# 		json.dump(my_bidding_list, outfile)

				except (FacebookRequestError,AttributeError) as error:
					list=[]
					#print (error)
					return list
				
					#print (error.api_error_code())

		def get_ad_info(self, adsets):

				#set params for getting info about ad sets
				params = {  'date_preset' :'last_28_days',	
						'time_increment' : '1'					
				}

				for adset in adsets:
					#print (adsets)
					path = 'https://graph.facebook.com/v2.3/%s/insights?limit=500' % (adset)
					response = self.api.call('GET', path, params = params)
					
					stats = response.json()
					my_ads_list = json.loads(json.dumps(stats))
					pprint.pprint (my_ads_list)
					#print(len(my_ads_list))

					#with open('ads_data.json', 'a') as outfile:
					#	json.dump(my_ads_list, outfile)
					return my_ads_list
#if __name__ == '__main__':

	#in final version, read the values from txt file
	#a.get_bidding_info()
	#a.get_ad_info()

#pprint.pprint (a.get_stats())
#print (len(a.get_stats()))


