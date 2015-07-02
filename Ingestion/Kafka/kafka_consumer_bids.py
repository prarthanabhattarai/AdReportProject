import time
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
import os


class Consumer(object):

    def __init__(self, addr, group, topic):
        self.client = KafkaClient(addr)
        self.consumer = SimpleConsumer(self.client, group, topic, max_buffer_size=1310720000)
        self.temp_file_path = None
        self.temp_file = None
        self.hadoop_path = "/user/AdReport/%s/history" %(topic)
        self.cached_path = "/user/AdReport/%s/cached" % (topic)
        self.topic = topic
        self.group = group
        self.block_cnt = 0


    def consume_topic(self, output_dir):

        timestamp = time.strftime('%Y%m%d%H%M%S')
        
        #open file for writing
        self.temp_file_path = "%s/kafka_%s_%s_%s.dat" % (output_dir,
                                                         self.topic,
                                                         self.group,
                                                         timestamp)
        self.temp_file = open(self.temp_file_path,"w")
	print ( self.temp_file) 
	#one_entry = False

        while True:
            try:
                messages = self.consumer.get_messages(count=10, block=False)
		
                #OffsetAndMessage(offset=43, message=Message(magic=0,
                # attributes=0, key=None, value='some message'))
                for message in messages:
		    print (message)
		    #one_entry = True
                    #print (self.temp_file.tell())
		    self.temp_file.write(message.message.value + "\n")		

                if self.temp_file.tell() > 2000000:
                    self.save_to_hdfs(output_dir)

                self.consumer.commit()
            except:
                self.consumer.seek(0, 2)

	#if one_entry:
	    #print ("sending to hdfs")
            #self.save_to_hdfs(output_dir, self.topic)
	#self.consumer.commit()

    def save_to_hdfs(self, output_dir):
	print ("Saving file to hdfs")
        self.temp_file.close()
	print ("Closed open file")
        timestamp = time.strftime('%Y%m%d%H%M%S')

        hadoop_fullpath = "%s/%s_%s_%s.dat" % (self.hadoop_path, self.group,
                                               self.topic, timestamp)
        cached_fullpath = "%s/%s_%s_%s.dat" % (self.cached_path, self.group,
                                               self.topic, timestamp)
        #print ("Block " + str(self.block_cnt) + ": Saving file to HDFS " + hadoop_fullpath)
        self.block_cnt += 1

        # place blocked messages into history and cached folders on hdfs
        os.system("sudo -u ubuntu /usr/local/hadoop/bin/hdfs dfs -put %s %s" % (self.temp_file_path,
                                                        hadoop_fullpath))
        os.system("sudo -u ubuntu /usr/local/hadoop/bin/hdfs dfs -put %s %s" % (self.temp_file_path,
                                                        cached_fullpath))
        os.remove(self.temp_file_path)

        timestamp = time.strftime('%Y%m%d%H%M%S')

        self.temp_file_path = "%s/kafka_%s_%s_%s.dat" % (output_dir,
                                                         self.topic,
                                                         self.group,
                                                         timestamp)
        self.temp_file = open(self.temp_file_path, "w")

if __name__ == '__main__':
    
    print "\nConsuming messages for bids..."
    cons = Consumer(addr="52.8.165.110", group="hdfs", topic="fb_bids")
    cons.consume_topic("/home/ubuntu/user/AdReport/kafka_messages_bids")

