import socket

class FS_Node:
	def __init__(self,address):
		self.files = {}

	def get_files(self):
		files = []
		for file in self.files:
			files[file] = self.files[file]
		return files

	def add_file(self, file, num_packets, packets_owned):
		self.files[file] = (num_packets, packets_owned)
	
