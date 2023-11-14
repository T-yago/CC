import threading

from ReentrantRWLock import ReentrantRWLock

class FS_Tracker_DataBase():

	"""
	Estrutura f_complete = {F1: [LOCK, 0, 20, 172.0.1, 193.0.1.2]}
	A key corresponde ao nome do ficheiro e o value corresponde a uma lista dos endereços IPv4 dos FS_Nodes que
	possuem esse ficheiro completo, sendo o primeiro elemento da lista um lock para controlar as leituras e escritas,
	e o segundo elemento o número de pacotes que compõem o ficheiro.

	Estrutura f_incomplete = {F1: [LOCK, 1, 20, [172.0.1, 61253], [193.0.1.2, 5723]]}
	A key corresponde ao nome do ficheiro e o value correspode a uma lista em que o primeiro elemento é um lock para controlar as leituras e escritas
	e o segundo é sempre o número de pacotes em que o ficheiro está dividido e os restantes elementos correspondem aos pacotes que cada
	FS_Node possui e o endereço IPv4 de cada FS_Node.
	
	Importante realçar que os inteiros associados aos endereços IPv4 correspondem aos pacotes que cada FS_Node possuí
	quando convertidos para binário. Por exemplo, 61253 corresponde a 1110111101000101 em binário e, sabendo que o ficheiro está
	dividido em 20 pacotes no total, acrescentamos 0 à esquerda até termos 20 dígitos, resultando no número 00001110111101000101.
	Desta forma, concluímos que faltam 10 pacotes (contar os 0s) para o correspondente FS_Node ter o ficheiro completo.
	"""
	def __init__(self):
		self.f_complete = {}
		self.f_incomplete = {}
		self.lock = threading.Lock()
	
	"""
	Adiciona a informação de um novo FS_Node às estruturas do FS_Tracker (ficheiros ou partes de ficheiros que este possuí).

	Importante realçar que o lock do FS_Tracker é usado para prevenir que dois FS_Nodes tentem criar o mesmo ficheiro ao mesmo tempo
	quando este ainda não existia no FS_Tracker. De igual modo, os locks de escrita são usados para prevenir que dois ou mais FS_Nodes
	tentem adicionar informações ao mesmo tempo na lista de informações do ficheiro, evitando, por exemplo, que escrevam na mesma posição
	da lista.
	"""
	def handle_data(self, addr, data):

		"""
		file -> String com o nome do ficheiro
		packet -> É um tuplo de dois elementos onde o primeiro elemento indica o número de pacotes em que o ficheiro está
		dividido e o segundo elemento é um inteiro representado em binário que corresponde aos pacotes que o FS_Node possuí.
		
		Exemplos:
		
		Se o ficheiro for completo -> [(file_name, 10, -1),...],
		Se for incompleto -> [(file_name, 15, 12345),...]
		"""
		for file in data:

			# Adiciona o ficheiro caso este seja novo na base de dados e insere logo o tamanho do ficheiro na primeira posição das listas
			self.lock.acquire()
			if file[0] not in self.f_complete:
				self.f_complete[file[0]] = [ReentrantRWLock(), 0, file[1]]
				self.f_incomplete[file[0]] = [ReentrantRWLock(), 0, file[1]]
			self.lock.release()
			
			# Verifica se o Node possuí o ficheiro completo
			if file[2] == -1:
				with self.f_complete[file[0]][0].w_locked():
					self.f_complete[file[0]].append(addr)
			# Adiciona um novo ficheiro incompleto caso contrário
			else:
				with self.f_incomplete[file[0]][0].w_locked():
					self.f_incomplete[file[0]].append([addr, file[2]])
	
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

	O XOR aqui é utilizado para fazer as operações de adição e remoção de pacotes de um ficheiro. Mas a informação que ele está à espera de receber do Node, não 
    é uma atulização do ficheiro, como 00100 -> 00110, caso tenha ocurrido adição, mas sim o registo da modificação que foi feita (onde foi feita)
	Por exemplo, 00100 -> 00010 significa que o Node fez uma alteração no 4º bit, e como sabemos que ele não tinha o 4º bit, ao realizar o XOR, o resultado será 00110.
	Se ele já tivesse o 4º bit, por exemplo, 00110 -> 00010 signficaria remover o 4º bit, e não adicionar, logo o resultado seria 00100.

	Em relação aos locks, o lock do FS_Tracker é usado para evitar o mesmo que na função handle_data, que é dois ou mais FS_Nodes
	tentarem criar o mesmo ficheiro ao mesmo tempo quando este ainda não existia no FS_Tracker. Por sua vez, os locks de escrita, que cada key
	do dicionário possuí, são usados para assegurar que quando estamos a fazer alterações relativamente aos pacotes que um FS_Node possuí de um ficheiro, a
	localização dessa informação (indíce na lista associada ao ficheiro) não se altera. Isso poderia acontecer, caso não usassemos locks de escrita e, por exemplo,
	outra thread removesse um FS_Node do mesmo ficheiro que estavamos a alterar, podendo assim alterar o indíce da lista referente ao FS_Node que estavamos
	a alterar.
	"""
	def update_information(self, addr, data):
		for file in data:
			
			self.lock.acquire()
			if file[0] in self.f_complete:
				self.lock.release()
				
				# Verifica se o FS_Node já possuía alguma informação relativa aquele ficheiro
				if addr not in self.f_complete[file[0]]:
					
					with self.f_incomplete[file[0]][0].w_locked():
						index = self.addr_has_packets(file[0], addr)
						if index == -1:
							# Verifica se o ficheiro está completo
							if (file[2] == -1):
								self.f_complete[file[0]].append(addr)
							else:
								self.f_incomplete[file[0]].append([addr, file[2]])
							self.lock.release()
						else:
							# Realizar a operação de ou exclusivo para fazer as adições e/ou deleções de pacotes de um ficheiro
							xor = self.f_incomplete[file[0]][index][1] ^ file[2]
							if (xor==0):
								del self.f_incomplete[file[0]][index]
							elif (xor==pow(2, 8 * file[1]) - 1):
								del self.f_incomplete[file[0]][index]
								self.f_complete[file[0]].append(addr)
							else:
								self.f_incomplete[file[0]][index][1] = xor
				else:
					if file[2] == -1:
						with self.f_complete[file[0]][0].w_locked():
							self.f_complete[file[0]].remove(addr)
					else:
						# Realizar a operação de ou exclusivo para fazer as deleções de pacotes de um ficheiro completo
						file_complete = pow(2, 8 * file[1]) - 1
						xor = file_complete ^ file[2]

						# Passar para o dicionário de ficheiros incompletos
						with self.f_complete[file[0]][0].w_locked():
							self.f_complete[file[0]].remove(addr)
							with self.f_incomplete[file[0]][0].w_locked():
								self.f_incomplete[file[0]].append([addr, xor])

			# Adicionar a informação caso o FS_Node não tivesse nada relativo aquele ficheiro
			else:
				self.f_complete[file[0]] = [ReentrantRWLock(), 0, file[1]]
				self.f_incomplete[file[0]] = [ReentrantRWLock(), 0, file[1]]
				self.lock.release()

				# Verifica se o Node possuí o ficheiro completo
				if file[2] == -1:
					with self.complete[file[0]][0].w_locked:
						self.complete[file[0]].append(addr)
				# Adiciona um novo ficheiro incompleto caso contrário
				else:
					with self.f_incomplete[file[0]][0].w_locked:
						self.f_incomplete[file[0]].append([addr, file[2]])
	
	"""
	Verifica se o FS_Node já tem pacotes de um determinado ficheiro
	"""
	def addr_has_packets(self, file, addr):
		for i in range(len(self.f_incomplete[file])-3):
			if (self.f_incomplete[file][i+3][0] == addr):
				return i+3
		return -1

	"""
	Devolve uma lista de tuplos, em que o primeiro elemento da lista é o tamanho do ficheiro e
	os tuplos são compostos por dois elementos em que o primeiro elemento é o endereço IPv4 do FS_Node que
	possuí pacotes do ficheiro solicitado e o segundo argumento são os pacotes correspondentes que possuí. Quando
	o caMpo dos pacotes que possuí for igual a -1, significa que o FS_Node contêm o ficheiro completo.

	Os locks de leitura, servem para impedir que funções que alterem as informações do ficheiros, alterem essa informação
	enquanto estamos a ler, prevenindo que apareça resultados errados.
	"""
	def get_file_owners(self, file):

		flag_alteraIncompletos = 0
		lista_inicial_completos = []
		lista_final_completos = []
		lista_inicial_incompletos = []
		lista_final_incompletos = []

		# Verificar se o ficheiro já existiu na rede
		if file not in self.f_complete:
			return lista_inicial_completos
		else:

			lista_inicial_completos.append(self.f_complete[file][1])
			# Percorre os FS_Nodes com o ficheiro completo
			with self.f_incomplete[file][0].r_locked():
				with self.f_complete[file][0].r_locked():

					for node in self.f_complete[file][3:][self.f_complete[1]:]:
						lista_inicial_completos.append((node, -1))
					
					for node in self.f_complete[file][3:][0:self.f_complete[1]]:
						lista_final_completos.append((node, -1))
					
					if (self.f_complete[1]<len(self.f_complete[3:])):
						self.f_complete[1] += 1
					elif (self.f_incomplete[1]<len(self.f_incomplete[3:])):
						flag_alteraIncompletos = 1
					else:
						self.f_complete[1] = 0
						flag_alteraIncompletos = 2
				
				# Percorre os FS_Nodes com o ficheiro incompleto
				for node in self.f_incomplete[file][2:][self.f_incomplete[file][1]:]:
					lista_inicial_incompletos.append(tuple(node))
				
				for node in self.f_incomplete[file][2:][0:self.f_incomplete[file][1]]:
					lista_final_incompletos.append(tuple(node))
				
				if (flag_alteraIncompletos==1):
					self.f_complete[1] += 1
				elif (flag_alteraIncompletos==2):
					self.f_incomplete[1] = 0
			
			lista = lista_inicial_completos + lista_inicial_incompletos + lista_final_incompletos + lista_final_completos
		
		return lista

	"""
	Remove os dados relacionados a um FS_Node.

	Locks são responsáveis por evitar leituras nas localizações que estamos a alterar.
	"""
	def remove_FS_node(self, addr):

		for file in self.f_complete:
			with self.f_complete[file][0].w_locked():
				if addr in self.f_complete[file]:
					self.f_complete[file].remove(addr)
		
		for file in self.f_incomplete:
			with self.f_incomplete[file][0].w_locked():
				index = self.addr_has_packets(file, addr)
				if index != -1:
					del self.f_incomplete[file][index]

