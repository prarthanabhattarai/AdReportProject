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

        with open('AdReports/FB_Report/accessToken.txt', 'rb') as f:
            self.access_token_list = pickle.load(f)

        with open('AdReports/FB_Report/accountID.txt', 'rb') as d:
            self.account_id_list = pickle.load(d)

    def produceMsg(self):
	
	for i in range ((len(self.access_token_list)):
	    #print (i)
	    a=apiReader(self.access_token_list[b],self.account_id_list[b])
            bidding_list = a.get_bidding_info()
            my_bid_list = json.dumps(bidding_list)
				
            if  len(bidding_list) > 1:
		
		my_active_ads=[]
                for i in range(len(bidding_list)):
                    my_active_ads.append(bidding_list[i]['id'])
                
           	try:
			self.producer.send_messages("fb_bids", my_bid_list)

                except (MessageSizeTooLargeError) as error:
			print (error)

		for ads in my_active_ads:
			ads_info = a.get_ad_info(ads)
               		my_ads_info= json.dumps(ads_info)

			if len(my_ads_info)>1:
				self.producer.send_messages("ads_info", my_ads_info)
	 
	time.sleep(SLEEP_TIMER)

if __name__ == "__main__":
    statsProducer = kafkaProducer('52.8.165.110')
    
    #once a day 
    SLEEP_TIMER = 86400
    while True:              
    	statsProducer.produceMsg()
