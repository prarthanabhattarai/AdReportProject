from getvalues import valuesReader
from getdata import apiReader
import pickle
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
from kafka.common import MessageSizeTooLargeError
import json

class kafkaProducer(object):

    def __init__(self, addr):
        self.client = KafkaClient(addr)
        self.producer = SimpleProducer(self.client)

        with open('AdReports/Facebook_Report/accessToken.txt', 'rb') as f:
            self.access_token_list = pickle.load(f)

        with open('AdReports/Facebook_Report/accountID.txt', 'rb') as d:
            self.account_id_list = pickle.load(d)

    def produceMsg(self):

        for i in range (len(self.access_token_list)):
        #for i in range (100) 
	    print (i)
	    a=apiReader(self.access_token_list[i],self.account_id_list[i])
            bidding_list = a.get_bidding_info()
            my_bid_list = json.dumps(bidding_list['data'])
				
            if  len(bidding_list) > 1:
		
		my_active_ads=[]
                for i in range(len(bidding_list['data'])):
                    my_active_ads.append(bidding_list['data'][i]['id'])
                
		ads_info_list = a.get_ad_info(my_active_ads)
		my_info_list= json.dumps(ads_info_list['data'])

           	try:
			self.producer.send_messages("fb_bids", my_bid_list)

                except (MessageSizeTooLargeError) as error:
			print (error)

		if len(my_info_list) > 1:
			self.producer.send_messages("ads_info", my_info_list)


if __name__ == "__main__":
    statsProducer = kafkaProducer('52.8.165.110')
    statsProducer.produceMsg()
