import socket

import os


class FS_Node:
	

	"""
	Cada node é inicializado com um endereço IPv4 e um nome de uma pasta que contêm os ficheiros que o node possuí
	Adicionalmente, deve ser feita a averiguação se o ficheiro de metadados, onde se guardam os dados sobre os ficheiros que existem, existe.
	Se existir, então é porque a aplicação já correu pelo menos uma vez e deve-se ler o ficheiro e preencher o dicionário com a informação sobre que pacotes cada ficheiro tem, nesse node.
	Caso contrário, sabemos que é a primeira vez que o código corre, logo podemos assumir que todos os ficheiros que são dados ao node estão completos.
	
	"""
	def __init__(self, address, folder_name):
		self.addr = address
		self.files = {}  
		self.permissions = []  

		# Open the specified folder and add its files to the 'files' dictionary
		folder_path = os.path.abspath(folder_name)

		if os.path.exists(folder_path) and os.path.isdir(folder_path):
			for file_name in os.listdir(folder_path):
				file_path = os.path.join(folder_path, file_name)
				if os.path.isfile(file_path):
					file_size = os.path.getsize(file_path)
					if os.path.isfile(path_to_metadata):
						## ler a struct e preencher o dicionário com os pacotes que cada ficheiro tem
						self.files[file_name] = (file_size, packets_owned)


					
					else self.files[file_name] = (file_size, -1)
					
		else:
			print(f"Folder '{folder_name}' does not exist.")
	


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


	def save_file (self, )
		




