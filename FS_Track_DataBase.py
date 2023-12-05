"""
Ficheiro correspondente à classe que funcionará como base de dados do FS_Tracker guardando toda a informação sobre
os FS_Nodes ligados ao mesmo, como, por exemplo, os ficheiros e partes de ficheiros que estes possuem. A esta
classe temos associadas funções de adição de ficheiros, remoção de ficheiros e atualização de ficheiros, de forma
a fazer uma gestão da base de dados.
"""

import threading
from ReentrantRWLock import ReentrantRWLock



class FS_Tracker_DataBase():

	"""
	Estrutura f_complete = {F1: [LOCK, 0, 20, 172.0.0.1, 193.0.1.2]}
	A key corresponde ao nome do ficheiro (exemplo: file1.txt) e o value corresponde a uma lista, onde, a partir do
	4 indíce, os elementos correspondem todos a endereços IPv4 dos FS_Nodes que possuem esse ficheiro completo. Relativamente
	aos restantes elementos da lista, o primeiro corresponde a um RWLOCK que previne que duas threads escrevam simultaneamente
	na informação deste ficheiro, ou que haja leituras em simultâneo com escritas. Por sua vez, o segundo elemento corresponde
	ao indíce do endereço IP que será enviado em primeiro lugar quando um FS_Node requesitar informação sobre que FS_Nodes
	possuem determinado ficheiro, por exemplo, para a lista acima, se for igual a 1, a lista dos IPs será, [193.0.1.2, 172.0.1].
	Depois, o terceiro elemento é o número de pacotes que o ficheiro possuí.

	
	Estrutura f_incomplete = {F1: [LOCK, 1, 20, [172.0.1, 61253], [193.0.1.2, 5723]]}
	Semelhante ao dicionário dos ficheiros completos, a key corresponde ao nome do ficheiro e o value a uma lista, onde, a partir
	da terceira posição todos os elementos correspondem a listas de dois elementos, onde o primeiro é um endereço IP e o segundo
	os pacotes que o FS_Node com esse endereço IP possuí daquele ficheiro. De salientar que neste dicionário não temos informação
	sobre o número de bytes do último pacote, logo temos de ir buscar essa informação ao dicionário dos ficheiros completos.
	
	Realçamos que os inteiros associados aos endereços IPv4 correspondem aos pacotes que cada FS_Node possuí quando convertidos
	para binário. Por exemplo, 61253 corresponde a 1110111101000101 em binário e, sabendo que o ficheiro está dividido em 20 pacotes
	no total, acrescentamos 0s à esquerda até termos 20 dígitos, resultando no número 00001110111101000101. Desta forma, concluímos
	que faltam 10 pacotes (contar os 0s) para o correspondente FS_Node ter o ficheiro completo.

	Uma instância desta classe tem ainda associado um LOCK da biblioteca threading de forma a controlar as alterações nos dicionários
	de ficheiros completos e incompletos, por exemplo, quando queremos adicionar um novo ficheiro.
	"""
	def __init__(self):
		self.f_complete = {}
		self.f_incomplete = {}
		self.lock = threading.Lock()


	"""
	Função responsável por atualizar os ficheiros e pacotes de ficheiros que um FS_Node possuí. Esta pode receber informação relativa
	a um único pacote de um ficheiro ou múltiplos, dependendo se o FS_Node está a fazer a primeira troca de informação com o FS_Tracker
	(podem ser múltiplos ficheiros) ou se é apenas uma atualização dos ficheiros que este já possuí (apenas 1 ficheiro).

	O argumento "addr" é o endereço IPv4 do FS_Node que está a informar da atualização de dados e o argumento "data" é uma lista de tuplos,
	sendo que cada tuplo possuí 4 elementos. O primeiro elemento é o nome do ficheiro, o segundo o número de pacotes que compõem o ficheiro,
	o terceiro o número de bytes do último pacote e o quarto os pacotes que o FS_Node possuí do ficheiro.

	Exemplo:

	Se o ficheiro for completo -> [(file_name, 10, -1),...],
	Se for incompleto -> [(file_name, 15, 12345),...]


	Caso o FS_Node já estivesse registado como detentor daquele pacote, o FS_Tracker assume que o FS_Node apagou esse pacote.
	Por outro lado, caso o FS_Tracker tivesse registado que o FS_Node ainda não possuía esse pacote, então atualiza os 	pacotes que o
	FS_Node possuí desse ficheiro e, caso o ficheiro fique completo, passa o FS_Node para o dicionário de ficheiros completos e remove
	a informação no dicionário de ficheiros incompletos.

	Caso o FS_Node já possuí-se o ficheiro completo e apaga-se um dos pacotes ou vários pacotes, então o FS_Tracker remove o FS_Node
	da lista dos FS_Nodes com o correspondente ficheiro completo e volta a passá-lo, para o dicionário de ficheiros incompletos.

	
	O XOR aqui é utilizado para fazer as operações de adição e remoção de pacotes de um ficheiro. Mas a informação que ele está à espera
	de receber do Node, não é uma atulização do ficheiro, como 00100 -> 00110, caso tenha ocurrido adição, mas sim o registo da modificação
	que foi feita (onde foi feita). Por exemplo, 00100 -> 00010 significa que o Node fez uma alteração no 4º bit, e como sabemos que ele não
	tinha o 4º bit, ao realizar o XOR, o resultado será 00110. Se ele já tivesse o 4º bit, por exemplo, 00110 -> 00010 signficaria remover o
	4º bit, e não adicionar, logo o resultado seria 00100.

	
	Em relação aos locks, o lock do FS_Tracker é usado para evitar que dois ou mais FS_Nodes tentem criar o mesmo ficheiro ao mesmo
	tempo quando este ainda não existia no FS_Tracker. Por sua vez, os locks de escrita, que cada key do dicionário tem associado asseguram
	que não resultam informações falsas provenientes de múltiplas escritas em simultâneo.
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
							if (file[2] == -1) or (file[1]==1):
								with self.f_complete[file[0]][0].w_locked():
									self.f_complete[file[0]].insert(self.f_complete[file[0]][1] + 3, addr)
							else:
								self.f_incomplete[file[0]].insert(self.f_incomplete[file[0]][1] + 3, [addr, file[2]])
						else:
							# Realizar a operação de ou exclusivo para fazer as adições e/ou deleções de pacotes de um ficheiro
							xor = self.f_incomplete[file[0]][index][1] ^ file[2]
							if (xor==0):
								del self.f_incomplete[file[0]][index]
							elif (xor==pow(2, file[1]) - 1):
								del self.f_incomplete[file[0]][index]
								with self.f_complete[file[0]][0].w_locked():
									self.f_complete[file[0]].insert(self.f_complete[file[0]][1] + 3, addr)
							else:
								self.f_incomplete[file[0]][index][1] = xor
				else:
					if file[2] == -1:
						with self.f_complete[file[0]][0].w_locked():
							self.f_complete[file[0]].remove(addr)
					else:
						# Realizar a operação de ou exclusivo para fazer as deleções de pacotes de um ficheiro completo
						file_complete = pow(2, file[1]) - 1
						xor = file_complete ^ file[2]

						# Passar para o dicionário de ficheiros incompletos
						with self.f_complete[file[0]][0].w_locked():
							self.f_complete[file[0]].remove(addr)
							with self.f_incomplete[file[0]][0].w_locked():
								self.f_incomplete[file[0]].insert(self.f_incomplete[file[0]][1] + 3, [addr, xor])

			# Adicionar a informação caso o FS_Node não tivesse nada relativo aquele ficheiro
			else:
				self.f_complete[file[0]] = [ReentrantRWLock(), 0, file[1]]
				self.f_incomplete[file[0]] = [ReentrantRWLock(), 0, file[1]]
				self.lock.release()

				# Verifica se o Node possuí o ficheiro completo
				if file[2] == -1:
					with self.f_complete[file[0]][0].w_locked():
						self.f_complete[file[0]].insert(self.f_complete[file[0]][1] + 3, addr)
				# Adiciona um novo ficheiro incompleto caso contrário
				else:
					with self.f_incomplete[file[0]][0].w_locked():
						self.f_incomplete[file[0]].insert(self.f_incomplete[file[0]][1] + 3, [addr, file[2]])


	"""
	Função auxiliar que verifica se o FS_Node já tem pacotes de um determinado ficheiro e, em caso afirmativo, devolve a posição
	onde se encontra a informação relativa aos pacotes que o mesmo possuí.
	"""
	def addr_has_packets(self, file, addr):
		for i in range(len(self.f_incomplete[file])-3):
			if (self.f_incomplete[file][i+3][0] == addr):
				return i+3
		return -1


	"""
	Devolve uma lista de tuplos, em que o primeiro elemento da lista é o tamanho do ficheiro, o segundo o tamnho em bytes do último pacote
	e os restantes são tuplos compostos por dois elementos em que o primeiro elemento é o endereço IPv4 do FS_Node que possuí pacotes do
	ficheiro solicitado e o segundo argumento são os pacotes correspondentes que possuí. Quando o campo dos pacotes que possuí for igual a -1,
	significa que o FS_Node contem o ficheiro completo.

	Os locks de leitura, servem para impedir que funções que alterem as informações dos ficheiros, alterem essa informação enquanto estamos a ler,
	prevenindo que apareça resultados errados.


	Importante realçar que em vez de enviarmos sempre os FS_Nodes que possuem determinado ficheiro pela ordem que os temos na nossa base de dados,
	vamos rodando essa lista. Este mecanismo em conjunto com um outro mecanismo executado pelos FS_Nodes (estratégia dos pacotes raros) serve para
	balancear a rede e prevenir que os FS_Nodes sejam sobrecarregados com pedidos.

	Desta forma, a cada pedido, é atualizada uma variável na base de dados, informando que no próximo pedido a lista terá de rodar mais uma unidade.
	Salientamos que quando adicionamos um novo FS_Node a um dos dicionários este é sempre adicionado no indíce indicado pela variável de rotação
	assegurando que no próximo pedido este é o primeiro elemento da lista. Fazemos isto, para quando o FS_Node for novo na rede, este ser o primeiro
	a ser requisitado quando alguém quer pacotes de um ficheiro que ele possuí.
	"""
	def get_file_owners(self, file):

		# Verificar se o ficheiro já existiu na rede
		if file not in self.f_complete:
			return []
		else:

			# Devolve a lista dos FS Nodes que possuem os ficheiros ou partes do mesmo rodando esta lista a cada pedido
			with self.f_incomplete[file][0].r_locked():
				with self.f_complete[file][0].r_locked():

					list1 = self.f_complete[file][3:]
					list2 = self.f_incomplete[file][3:]
					rotations_l1 = self.f_complete[file][1]
					rotations_l2 = self.f_incomplete[file][1]

					if (rotations_l1<len(list1)):
						lista = list1[rotations_l1:] + list2 +  list1[:rotations_l1]
					else:
						lista = list2[rotations_l2:] + list1 +  list2[:rotations_l2]

					if (rotations_l1<len(list1)):
						self.f_complete[file][1] += 1
					elif (rotations_l2<len(list2)-1):
						self.f_incomplete[file][1] += 1
					else:
						self.f_complete[file][1] = 0
						self.f_incomplete[file][1] = 0
		
		return [self.f_complete[file][2]] + lista


	"""
	Função que remove os dados relacionados a um FS_Node, por exemplo, quando este se disconecta da rede.

	Quando a informação relativa ao FS_Node é removida é tido em conta qual o indíce de rotação associado ao ficheiro. Caso o indíce da informação
	seja inferior ao indíce da rotação, então a informação é removida e o indíce de rotação é decrementado uma unidade, assegurando que o endereço
	que tinha sido enviado em primeiro lugar no pedido anterior, não é enviado em primeiro lugar novamente. Caso contrário, o indíce de rotação não
	é alterado.
	"""
	def remove_FS_node(self, addr):

		for file in self.f_complete:
			with self.f_complete[file][0].w_locked():

				if addr in self.f_complete[file]:
					index_addr = self.f_complete[file].index(addr) - 3
					self.f_complete[file].remove(addr)

					if (index_addr<self.f_complete[file][1]):
						self.f_complete[file][1] -= 1
		
		for file in self.f_incomplete:
			with self.f_incomplete[file][0].w_locked():
				index = self.addr_has_packets(file, addr)
				if index != -1:
					del self.f_incomplete[file][index]

					if (index-3<self.f_incomplete[file][1]):
						self.f_incomplete[file][1] -= 1
	
	"""
	Função que devolve o número de pacotes que compõem determinado ficheiro
	"""
	def get_size_file(self, file):
		return self.f_complete.get(file)[2]





	def print_dic(self):
		
		print("----------Completos----------")
		for file  in self.f_complete:
			print(file)
			print(self.f_complete[file])
		print("----------Incompletos----------")
		for file  in self.f_incomplete:
			print(file)
			print(self.f_incomplete[file])
		print("")