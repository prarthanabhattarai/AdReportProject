from getvalues import valuesReader
from getdata import apiReader
import pickle

with open('AdReports/FB_Report/accessToken.txt', 'rb') as f:
	access_token_list = pickle.load(f)

with open('AdReports/FB_Report/accountID.txt', 'rb') as d:
	account_id_list = pickle.load(d)

for i in range (len(access_token_list)):
#for i in range (10):
	print (i)
	a=apiReader(access_token_list[i],account_id_list[i])
	bidding_list = a.get_bidding_info()

	if len(bidding_list) > 1:
		
		my_active_ads = []
		for i in range(len(bidding_list['data'])):

			my_active_ads.append(bidding_list['data'][i]['id'])
		
		a.get_ad_info(my_active_ads)
