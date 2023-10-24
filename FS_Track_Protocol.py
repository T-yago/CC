
class FS_Tracker:

	"""
	Estrutura f_complete = {F1: [20, 172.0.1, 193.0.1.2]}
	A key corresponde ao nome do ficheiro e o value corresponde a uma lista dos endereços IPv4 dos FS_Nodes que
	possuem esse ficheiro completo, sendo o primeiro elemento da lista o número de pacotes que compõem o ficheiro.

	Estrutura f_incomplete = {F1: [20, [172.0.1, 61253], [193.0.1.2, 5723]]}
	A key corresponde ao nome do ficheiro e o value correspode a uma lista em que o primeiro elemento é sempre
	o número de pacotes em que o ficheiro está dividido e os restantes elementos correspondem aos pacotes que cada
	FS_Node possuí e o endereço IPv4 de cada FS_Node.
	
	Importante realçar que os inteiros associados aos endereços IPv4 correspondem aos pacotes que cada FS_Node possuí
	quando convertidos para binário. Por exemplo, 61253 corresponde a 1110111101000101 em binário e, sabendo que o ficheiro está
	dividido em 30 pacotes no total, acrescentamos 0 à esquerda até termos 20 dígitos, resultando no número 00001110111101000101.
	Desta forma, concluímos que faltam 10 pacotes (contar os 0s) para o correspondente FS_Node ter o ficheiro completo.
	"""
	def __init__(self):
		self.f_complete = {}
		self.f_incomplete = {}
	
	"""
	Adiciona a informação de um novo FS_Node às estruturas do FS_Tracker (ficheiros ou partes de ficheiros que este possuí).
	"""
	def handle_data(self, addr, data):

		"""
		file -> String com o nome do ficheiro
		packet -> É um tuplo de dois elementos onde o primeiro elemento indica o número de pacotes em que o ficheiro está
		dividido e o segundo elemento é um inteiro representado em binário que corresponde aos pacotes que o FS_Node possuí.
		
		Exemplos:
		
		Se o ficheiro for completo -> {file: (10, -1)},
		Se for incompleto -> {file: (15, 12345)}
		"""
		for (file, packet) in data:
			# Adiciona o ficheiro caso este seja novo na base de dados e insere logo o tamanho do ficheiro na primeira posição das listas
			if file not in self.f_complete:
				self.f_complete[file] = [packet[0]]
				self.f_incomplete[file] = [packet[0]]
			
			# Verifica se o Node possuí o ficheiro completo
			if packet[1] == -1:
				self.complete[file].append(addr)
			# Adiciona um novo ficheiro incompleto caso contrário
			else:
				self.f_incomplete[file].append([addr, packet[1]])
	
	"""
	Função responsável por atualizar os ficheiros e pacotes de ficheiros que um FS_Node possuí. Esta pode
	receber informação relativa a um único pacote de um ficheiro ou múltiplos, dependendo se o FS_Node envia essa informnação
	sempre que recebe um novo pacote ou se envia apenas após receber alguns pacotes (de forma a diminuir o overhead na rede).

	Caso o FS_Node já estivesse registado como detentor daquele pacote, o FS_Tracker assume que o FS_Node apagou esse pacote.
	Por outro lado, caso o FS_Tracker tivesse registado que o FS_Node ainda não possuía esse pacote, então atualiza a os
	pacotes que o FS_Node possuí desse ficheiro e, caso o ficheiro fique completo, passa o FS_Node para o dicionário de ficheiros
	completos e remove a informação no dicionário de ficheiros incompletos.

	Caso o FS_Node já possuí-se o ficheiro completo e apaga um dos pacotes ou vários pacotes, então o FS_Tracker remove o FS_Node
	da lista dos FS_Nodes com o correspondente ficheiro completo e volta a passá-lo, para o dicionário de ficheiros incompletos.
	"""
	def add_new_information(self, addr, data):
		for (file, packet) in data:
			if file in self.f_complete:

				# Verifica se o FS_Node já possuía alguma informação relativa aquele ficheiro
				if addr not in self.f_complete[file]:

					index = addr_has_packets(file, addr)
					if index == -1:
						# Verifica se o ficheiro está completo
						if (packet[1] == -1):
							self.complete[file].append(addr)
						else:
							self.f_incomplete[file].append([addr, packet[1]])
					else:
						# Realizar a operação de ou exclusivo para fazer as adições e/ou deleções de pacotes de um ficheiro
						xor = self.f_incomplete[file][index][1] ^ packet[1]
						self.f_incomplete[file][index][1] = xor
				else:
					if packet[1] == -1:
						self.f_complete[file].remove(addr)
					else:
						# Realizar a operação de ou exclusivo para fazer as deleções de pacotes de um ficheiro completo
						file_complete = pow(2, 8 * packet[0]) - 1
						xor = file_complete ^ packet[1]

						# Passar para o dicionário de ficheiros incompletos
						self.f_complete[file].remove(addr)
						self.f_incomplete[file].append([addr, xor])

			# Adicionar a informação caso o FS_Node não tivesse nada relativo aquele ficheiro
			else:
				self.f_complete[file] = [packet[0]]
				self.f_incomplete[file] = [packet[0]]

				# Verifica se o Node possuí o ficheiro completo
				if packet[1] == -1:
					self.complete[file].append(addr)
				# Adiciona um novo ficheiro incompleto caso contrário
				else:
					self.f_incomplete[file].append([addr, packet[1]])
	
	"""
	Verifica se o FS_Node já tem pacotes de um determinado ficheiro
	"""
	def addr_has_packets(self, file, addr):
		for i in range(len(self.f_incomplete[file])-1):
			if (self.f_incomplete[file][i+1][0] == addr):
				return i+1
		return -1


	"""
	Devolve uma lista de tuplos, em que o primeiro elemento da lista é o tamanho do ficheiro e
	os tuplos são compostos por dois elementos em que o primeiro elemento é o endereço IPv4 do FS_Node que
	possuí pacotes do ficheiro solicitado e o segundo argumento são os pacotes correspondentes que possuí. Quando
	o caMpo dos pacotes que possuí for igual a -1, significa que o FS_Node contêm o ficheiro completo.
	"""
	def return_file_owners(self, file):
		lista = []

		# Verificar se o ficheiro já existiu na rede
		if file not in self.f_complete:
			return lista
		else:

			# Percorre os FS_Nodes com o ficheiro completo
			for node in self.f_complete[file]:
				lista.append((node, -1))

			# Percorre os FS_Nodes com o ficheiro incompleto
			for node in self.f_incomplete[file]-1:
				lista.append(tuple(node))
		
		return lista

	"""
	Remove os dados relacionados a um FS_Node
	"""
	def remove_FS_node(self, addr):

		for file in self.f_complete:
			if addr in self.f_complete[file]:
				self.f_complete[file].remove(addr)
		
		for file in self.f_incomplete:
			index = addr_has_packets(file, addr)
			if index != -1:
				del self.f_incomplete[file][index]
