import json

class valuesReader():

	def __init__(self, filename):
		self.inputfile = filename
		self.app_ids = []
		self.access_tokens = []
		self.account_id = []

		with open(self.inputfile) as data_file:    
			id_data = json.load(data_file)

		for i in range(len(id_data)):
			self.access_tokens.append(id_data[i]['access_token'])
			self.account_id.append(id_data[i]['account_id'])

	def get_access_tokens(self):
		return self.access_tokens

	def get_account_id(self):
		return self.account_id

# if __name__ == '__main__':

# 	my_values=valuesReader('test.txt')
# 	my_values.read_values_from_file()


