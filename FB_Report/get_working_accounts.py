from getvalues import valuesReader
from getdata import apiReader
import pickle

ids = valuesReader('test.txt')
access_token_list=ids.get_access_tokens()
account_id_list=ids.get_account_id()

print (len(access_token_list))

working_access_token=[]
working_account_id = []

for i in range((len(access_token_list))):

	a=apiReader(access_token_list[i],account_id_list[i])
	bidding_list = a.get_bidding_info()
	
	if bidding_list:

		working_access_token.append(access_token_list[i])
		working_account_id.append(account_id_list[i])

		if len(bidding_list) > 1:

			my_active_ads = []

			for i in range(len(bidding_list['data'])):

				my_active_ads.append(bidding_list['data'][i]['id'])

			a.get_ad_info(my_active_ads)

with open('accessToken.txt', 'wb') as outfile:
	pickle.dump(working_access_token, outfile, protocol=2)

with open('accountID.txt', 'wb') as outfile:
	pickle.dump(working_account_id, outfile, protocol=2)

with open('accessToken.txt', 'rb') as f:
    my_list_A = pickle.load(f)

with open('accountID.txt', 'rb') as d:
	my_list_B = pickle.load(d)

#print (len(my_list_A))
#print (len(my_list_B))


