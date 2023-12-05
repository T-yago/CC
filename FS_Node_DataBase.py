"""
Ficheiro correspondente à classe que funcionará como base de dados do FS_Node guardando toda a informação sobre
os ficheiros que o mesmo possuí. A esta classe temos associadas funções de adição e remoção de ficheiros e pacotes,
de forma, a fazer uma gestão da base de dados.
"""

import threading
from ReentrantRWLock import ReentrantRWLock



class FS_Node_DataBase():

	"""
	Estrutura que guarda a informação relativa aos ficheiros ou partes de ficheiros que o FS_Node possuí. Contem um
	dicionário em que cada key corresponde o nome do ficheiro e cada value associada à key é uma lista de 3 elementos.
	O primeiro elemento é um RWLock, para prevenir que duas threads alterem os dados relativos a um ficheiro em simultâneo,
	gerando resultados, errados. O segundo elemento é o número de pacotes em que o ficheiro está dividido e o terceiro é um
	inteiro representante dos pacotes que o FS_Node possuí.

	Exemplo da estrutura: {file1: [LOCK, 20, 74215]}

	Importante salientar que há ainda um lock associado de forma a prevenir que duas threads alterem a estrutura em simultâneo
	"""
	def __init__(self):
		self.files = {}
		self.lock = threading.Lock()


	"""
	Função que retorna a informação de todos os ficheiros que o FS_Node possuí, sob a forma de uma lista de tuplos com 3
	elementos cada. O primeiro elemnto é o nome do ficheiro, o segundo o número de pacotes em que o ficheiro está dividido
	e o terceiro o inteiro correspondente aos pacotes que o FS_Node possuí do ficheiro correspondente.

	Exemplo: (file1, 50, 65763)
	"""
	def get_files(self):
		files = []
		for file, info in self.files.items():
			files.append((file, info[1], info[2]))
		
		return files


	"""
	Função que adiciona uma lista de ficheiros ao dicionário relativo aos ficheiros que o FS_Node possuí.
	"""
	def add_files(self, files):
		for (file, num_packets, packets_owned) in files:
			self.lock.acquire()
			self.files[file] = [ReentrantRWLock(), num_packets, packets_owned]
			self.lock.release()

	"""
	Função que remove um ficheiro da lista de ficheiros
	"""
	def remove_file(self, file):
		self.lock.acquire()
		del self.files[file]
		self.lock.release()
	

	"""
	Função responsável por atualizar um pacote específico de um ficheiro, podendo ser uma adição ou deleção
	de um pacote.
	"""
	def update_packet(self, file, packet_index):
		self.lock.acquire()
		info = self.files.get(file)
		self.lock.release()
		
		packet_update_index = 1 << info[1] - packet_index - 1
		if info:
			with info[0].w_locked():
				packets_owned = info[2]
				new_value = packets_owned ^ packet_update_index
				if (new_value != pow(2, info[1]) - 1):
					info[2] = new_value
				else:
					info[2] = -1


	"""
	Função que verifica se o FS_Node já possuí um pacote específico de determinado ficheiro. Devolve True caso o FS_Node
	já possua o pacote e False caso não possua
	"""
	def check_packet_file(self, file, packet):
		self.lock.acquire()
		info = self.files.get(file)
		self.lock.release()

		if info:
			with info[0].r_locked():
				binary_to_compare = 1 << info[1] - packet - 1
				return (info[2] & binary_to_compare > 0)


	"""
	Função que devolve os nomes dos ficheiros, recebendo ainda um inteiro que determina se a função deverá retornar os nomes
	de todos os ficheiros, ou apenas os dos completos ou os dos incompletos.
	"""
	def get_files_names(self, condition):
		names_files = []

		if condition==0:
			self.lock.acquire()
			for file in self.files:
				names_files.append(file)
			self.lock.release()
		elif condition==1:
			self.lock.acquire()
			for file, info in self.files.items():
				if info[2]==-1:
					names_files.append(file)
			self.lock.release()
		elif condition==2:
			self.lock.acquire()
			for file, info in self.files.items():
				if info[2]!=-1:
					names_files.append(file)
			self.lock.release()

		return names_files


	"""
	Função que retorna um tuplo que representa quantos pacotes o FS_Node já possuí de determinado ficheiro. Desta forma,
	o primeiro elemento corresponde ao número de pacotes que o FS_Node possuí do ficheiro e o segundo elemento corresponde
	ao número de pacotes em que o ficheiro está dividido.
	"""
	def get_number_packets_completed(self, file):
		self.lock.acquire()
		info = self.files.get(file)
		self.lock.release()

		if info:
			with info[0].r_locked():
				if (info[2]==-1):
					return (info[1], info[1])
				else:
					completed_pcks = bin(info[2]).count('1')
					return (completed_pcks, info[1])
		else:
			return -1
	
	"""
	Função que devolve o inteiro que corresponde aos pacotes que o FS_Node possuí de determinado ficheiro
	"""
	def get_packets_file(self, file):
		self.lock.acquire()
		info = self.files.get(file)
		self.lock.release()

		if info:
			return info[2]
		else:
			return 0
	

	"""
	Função que devolve o número total de pacotes que compõem determinado ficheiro
	"""
	def get_size_file(self, file):
		return self.files.get(file)[1]


	""""
	Função associada a quando um cliente pede um ficheiro. Esta recebe uma lista em que o primeiro elemento é o número
	de pacotes em que o ficheiro está dividido e os restantes elementos correspondem a FS_Nodes que possuem o ficheiro ou
	pacotes do ficheiro requisitado pelo FS_Node. A lista recebida segue o seguinte formato:

	Exemplo: [10, (192.124.123,-1), (192.124.124, 123), (192.124.125, 123)]

	Desta forma, a função inicializa uma lista com um número de elementos igual ao número de pacotes do ficheiro e cada
	posição corresponderá ao número de pacotes que existem de cada pacote na rede, ou seja, se na posição 0 da lista estiver
	o número 2, então existem 2 FS_Nodes que possuem o primeiro pacote do ficheiro.

	Por fim, cria uma nova lista, em que em cada posição se encontra o número do pacote, e ordena-a de acordo com a lista anterior,
	ficando assim este ordenado por ordem crescente dos pacotes mais comuns.
	"""
	def get_rarest_packets (self, list_FS_Nodes) :

		# Dicionário que contêm o número de FS_Nodes que possuem o pacote com aquele índice
		packets = [0] * list_FS_Nodes[0]

		# Percorrer os bits que corresponde a cada pacote do ficheiro
		for i in range(len(packets)):
			# Percorrer todos os FS_Nodes que possuem o ficheiro, nesse bit e calcular a soma desse bit
			bit_sum = sum((node[1] >> i) & 1 for node in list_FS_Nodes[1:])
			packets[i] = bit_sum

		# Inverte a ordem do array, porque está ao contrário
		packets = packets[::-1]

		# Cria um array auxiliar com os índices do array, para ordenar
		packet_indices = list(range(len(packets)))

		sorted_indices = sorted(packet_indices, key=lambda x: packets[x])

		return sorted_indices
