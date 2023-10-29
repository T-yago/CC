import socket

class FS_Node:
	def __init__(self,address):
		self.addr = address
		self.files = {}
		self.peers = {}

	def get_files(self):
		files = {}
		for file in self.files:
			files[file] = self.files[file]
		return files

	def get_addr(self):
		return self.addr

	def set_addr(self, addr):
		self.addr = addr

	def add_file(self, file, num_packets, packets_owned):
		self.files[file] = (num_packets, packets_owned)

	"""
	Cria uma conexão entre o node e o servidor (TCP) e informa o servidor dos ficheiros que tem
	"""
	def conn_server(self, server_ip, server_port):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((server_ip, server_port))
		s.send_data(self.files)


	"""
	"""
	def send_data(self, files):
		try:
			if not s:
				raise ConnectionError("Socket is not connected to the server yet")
			# Send the data
			s.send(files)
		except ConnectionError as ce:	
			print(f"Connection error: {ce}")
		except Exception as e:
			print(f"An error occurred: {e}")
			
		

	
	
	
