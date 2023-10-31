import socket

class FS_Node_DataBase():
	def __init__(self,address):
		self.files = {}

	def get_files(self):
		files = []
		for file in self.files:
			files[file] = self.files[file]
		return files

	def add_file(self, file, num_packets, packets_owned):
		self.files[file] = (num_packets, packets_owned)
	
	"""
	Função que retorna a informação de todos os ficheiros que o FS_Node possuí
	"""
	def get_all_info(self):
		info = []

		for file, info_file in self.files:
			info.append((file, info_file[0], info_file[1]))
		return info

	"""
	Função que devolve os nomes dos ficheiros, recebendo ainda um inteiro que determina se a função
	deverá retornar os nomes de todos os ficheiros, ou apenas os dos completos ou os dos incompletos.
	"""
	def get_files(self, condition):
		names_files = []

		if condition==0:
			for file in self.files:
				names_files.append(file)
		elif condition==1:
			for file, info in self.files:
				if info[1]==-1:
					names_files.append(file)
		elif condition==2:
			for file, info in self.files:
				if info[1]!=-1:
					names_files.append(file)

		return names_files
	
	"""
	Função que retorna que percentagem do ficheiro já foi transferida para o FS_Node
	"""
	def get_number_packets_completed(self, file):
		if (info := self.files.get(file)):
			if (info[1]==-1):
				return (info[1], info[1])
			else:
				completed_pcks = bin(info[1]).count('1')
				return (completed_pcks, file[0])
		else:
			return -1
