"""
Ficheiro que executa o FS_Node.
"""

import socket
import threading
import signal
import sys
import os
import FS_Node_DataBase
import Message_Protocols



# Tamanho de cada pacote de um ficheiro
PACKET_SIZE = 144


"""
Funções responsáveis por atualizar os metadados dos ficheiros que o FS_Node possuí quando o cliente termina o programa
ao pressionar ctrl+c. É guardada a informação essencial para caso o cliente volte a iniciar o FS_Node, seja possível
reconstruir as estruturas relativas aos ficheiros que o mesmo possuí.

O ficheiro dos metadados segue a seguinte apresentação:

MetaDados.txt:

file1.txt		(nome do ficheiro)
50				(número de pacotes do ficheiro quando completo)
65763			(pacotes que possuí representado por um inteiro)
image1.png		(nome do ficheiro)
70				(número de pacotes do ficheiro quando completo)
451334			(pacotes que possuí representado por um inteiro)
...
"""
def signal_handler(sig, frame, dir, FS_Node_DB):
	write_MTDados(dir, FS_Node_DB)
	
	sys.exit(0)


"""
Função responsável por escrever os metadados dos ficheiros que o FS_Node possuí num ficheiro de metadados.
O ficheiro segue a estrutura:
MetaDados.txt:

file1.txt		(nome do ficheiro)
50				(número de pacotes do ficheiro quando completo)
65763			(pacotes que possuí representado por um inteiro)
file2.txt
...
"""
def write_MTDados(dir, FS_Node_DB):
	file_meta = dir + "MetaDados.txt"

	if os.path.exists(file_meta):
		os.remove(file_meta)

	with open(file_meta, "w") as file:
		for name, n_packets, packets in FS_Node_DB.get_files():
			file.write(f"{name}\n")
			file.write(f"{n_packets}\n")
			file.write(f"{packets}\n")


""""

A função get_file_metadata() recebe o nome de um ficheiro e o caminho para o ficheiro de metadados e devolve uma lista com os metadados desse ficheiro.
Assume ainda que os ficheiros de metadados seguem a seguinte apresentação:
MetaDados.txt:

file1.txt		(nome do ficheiro)
50				(número de pacotes do ficheiro quando completo)
65763			(pacotes que possuí representado por um inteiro)
file2.txt
...
"""

def get_file_metadata(meta_file):
	metadata = [] 
	file_index = 0
	current_index = 0

	with open(meta_file, "r") as file:
		lines = file.readlines()

	while current_index < len(lines):
		name = lines[current_index].strip()
		n_packets = int(lines[current_index + 1])
		packets = int(lines[current_index + 2])
		metadata[file_index] = [name, n_packets, packets]

		# Passa para a próxima linha onde pode estar o nome de um ficheiro
		current_index += 3
		
	return metadata

"""
Função responsável por devolver um dicionário com os ficheiros que o FS_Node possuí.
Primeiro, é verificado se existe um ficheiro de metadados.
Caso exista, então é lido e o dicionário é populado com os dados do ficheiro.
De seguida, verifica-se se existem ficheiros que não estão no ficheiro de metadados.
Caso existam, são adicionados ao dicionário, sendo assumindo que estão completos. 
"""
def fetch_files(dir, path_to_metadata):
	folder_path = os.path.abspath(dir)

	files = {}

	## Vê se existe ficheiro de metadados
	if os.path.exists(folder_path) and os.path.isdir(folder_path):

			if os.path.isfile(path_to_metadata):

				# se existe, popula o dicionário
				metadata = get_file_metadata(path_to_metadata)
				for name, n_packets, packets in metadata:
					files[name] = (name, n_packets, packets)
			
			# vê se existem ficheiros que não estão no ficheiro de metadados
			for file_name in os.listdir(folder_path):
				file_path = os.path.join(folder_path, file_name)
				if os.path.isfile(file_path) and file_name not in files:
					# Se existirem, adiciona-os ao dicionário, assumindo que estão completos
					file_size = os.path.getsize(file_path)
					files[file_name] = (file_name, file_size, -1)

	else:
		print(f"Folder '{dir}' does not exist.")

	return files


"""
Função que retorna uma lista ordenada por ordem descrescente de prioridade a quem o FS_Node deve pedir o
pacote que pretende. Esta percorre a lista que possuí a informação sobre os FS_Nodes que não possuem e que
possuem o pacote que FS_Node a partir da posição central e expandindo em redor da mesma. Este algoritmo, serve
para tentar escolher o FS_Node que se encontra a responder a uma menor quantidade de pedidos referentes ao
ficheiro que nós pretendemos. Assim, tentamos não sobrecarregar nenhum FS_Node.
"""
def FS_Nodes_ask_order(list_FS_Nodes_With_Packet):
	FS_Nodes_ask_order = []

	# Determina o ponto central da lista
	size_list = len(list_FS_Nodes_With_Packet)
	index_start_point = size_list // 2

	# Adiciona o ponto central, caso este contenha um endereço IP
	if ((ip := list_FS_Nodes_With_Packet[index_start_point])!=0):
		FS_Nodes_ask_order.append(ip)

	# Verifica se o número de elementos é par ou ímpar
	if (len(list_FS_Nodes_With_Packet) % 2 == 0):

		# Percorre a lista a partir do ponto central, adicionando à lista os endereços IP
		for i in range(1, index_start_point, 1):

			# Verifica se contêm um endereço ip nos índices i posições deslocados da posição inicial
			if ((ip := list_FS_Nodes_With_Packet[index_start_point-i])!=0):
				FS_Nodes_ask_order.append(ip)
			if ((ip := list_FS_Nodes_With_Packet[index_start_point+i])!=0):
				FS_Nodes_ask_order.append(ip)
		
		# Por o número de elementos ser par verificamos se o primeiro elemento da lista é um endereço IP
		if ((ip := list_FS_Nodes_With_Packet[0])!=0):
			FS_Nodes_ask_order.append(ip)
	
	else:

		# Percorre a lista a partir do ponto central, adicionando à lista os endereços IP
		for i in range(1, index_start_point + 1, 1):

			# Verifica se contêm um endereço ip nos índices i posições deslocados da posição inicial
			if ((ip := list_FS_Nodes_With_Packet[index_start_point-i])!=0):
				FS_Nodes_ask_order.append(ip)
			if ((ip := list_FS_Nodes_With_Packet[index_start_point+i])!=0):
				FS_Nodes_ask_order.append(ip)
	
	return FS_Nodes_ask_order



"""
Função que retorna um tuplo de dois elementos em que o primeiro elemento é o número de FS_Nodes que possuem
o pacote que o FS_Node necessita e, o segundo elemento, é uma lista em que cada posição corresponde a um
FS_Node, estando a 0 caso o FS_Node não possua o pacote pretendido e contendo o endereço IP caso contenha o
pacote pretendido.
"""
def FS_Nodes_with_packet(FS_Nodes, packet_to_check):

	# Lista com os FS_Nodes que possuem o pacote pretendido, estando a posição igual ao IP caso tenha o pacote e a 0 caso não tenha
	list = [0] * FS_Nodes[0]

	# Percorre todos os FS_Nodes à procura dos que têm o ficheiro
	index = 0
	for (ip, packets) in FS_Nodes[1:]:

		# Verifica se o FS_Node tem o pacote
		binary_to_compare = 1 << FS_Nodes[0] - packet_to_check - 1
		if (binary_to_compare & packets > 0):
			list[index] = ip
		index += 1
	
	return list


"""
Função responsável por obter os pacotes do ficheiro. Esta começa por escolher um pacote para obter, determina
qual o melhor FS_Node a quem pedir o pacote, tenta obter o pacote e, por fim, atualiza a informação da base de
dados do FS_Node e atualiza o ficheiro correspondente ao pacote.

Importnate salientar, que caso o timeout de espera para obter uma resposta de outro FS_Node seja ultrapassado 3
vezes, é escolhido outro FS_Node a quem pedir o pacote. Caso não consiga obter o pacote de nenhum FS_Node então
passa para outro pacote e no fim da transferência do ficheiro, avisa o utilizador que não foi possível obter
o ficheiro completo.
"""
def get_file_Thread(s, send_lock, FS_Node_DB, FS_Nodes, fileName, priority_queue, index, lock_priority_queue):

	# Cria um socket UDP para pedir os pacotes, tendo este um timeout de 2 segundos para prevenir esperas infinitas
	client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	client_socket.settimeout(2.0)
	
	# Obtem um dos pacotes do ficheiro
	lock_priority_queue.acquire()
	while (index<len(priority_queue)):
		packet_to_check = priority_queue[index]
		index += 1
		lock_priority_queue.release()

		# Verifica se o FS_Node já possuí esse pacote
		if (not FS_Node_DB.check_packet_file(fileName, packet_to_check)):

			# Determina quantos FS_Nodes e que FS_Nodes possuem o pacote
			list_FS_Nodes_With_Packet = FS_Nodes_with_packet(FS_Nodes, packet_to_check)

			# Determina a que FS_Nodes irá pedir os pacotes
			FS_Nodes_ask_order = FS_Nodes_ask_order(list_FS_Nodes_With_Packet)
			
			for FS_Node_ip in FS_Nodes_ask_order:

				# Pede o pacote ao FS_Node
				FS_Node_address = (FS_Node_ip, 9090)

				attempts = 0
				while attempts < 3:
					try:
						# Pede o ficheiro
						Message_Protocols.send_message_UDP(0, client_socket, fileName, FS_Node_address)
						received_packet_size_bytes, _ = client_socket.recvfrom(4)
						received_packet_size = int.from_bytes(received_packet_size_bytes, byteorder='big')

						# Guarda numa variável o pacote recebido
						packet = b''
						while len(packet) < received_packet_size:
							data = client_socket.recvfrom(received_packet_size - len(packet))
							if not data:
								return -1
							packet += data
						
						# Verifica se o FS_Node não mandou uma mensagem vazia, significando que já não possuí o pacote
						if received_packet_size > 0:
						
							# Guarda o pacote no ficheiro correspondente
							with open(fileName, 'w') as file:
								file.seek((index-1)*PACKET_SIZE)
								file.write(data)
							
							# Guarda o pacote na base de dados do FS_Node
							FS_Node_DB.update_packet(fileName, index-1)

							# Informa o FS_Tracker da atualização
							packet_update_index = 1 << len(priority_queue) - index - 2
							message = (fileName, len(priority_queue), packet_update_index)
							Message_Protocols.send_message_TCP(s, send_lock, message, True, 1)
						
						break

					except socket.timeout:
						attempts += 1

		lock_priority_queue.acquire()
	
	lock_priority_queue.release()

	# Fecha o socket UDP associado à transferência do ficheiro
	client_socket.close()


"""
Função responsável por fazer download de um ficheiro. Começa por pedir ao FS_Tracker os FS_Nodes que possuem pacotes
do ficheiro que pretende obter e depois cria uma lista ordenada por ordem crescente dos pacotes mais comuns na rede.
Por fim, cria X threads responsáveis por fazer o download do ficheiro.
"""
def downloadFile(s, send_lock, write_lock, threads_per_request, FS_Node_DB, fileName):

	# Pede ao FS_Tracker os FS_Nodes que possuem informação sobre o ficheiro
	send_lock.acquire()
	Message_Protocols.send_message(s, send_lock, fileName, True, 0)
	FS_Nodes = Message_Protocols.recieve_message(s, False)
	send_lock.release()

	# Cria uma lista para guardar as threads
	threads = []

	if len(FS_Nodes)>0:

		# Organiza a informação recebida pelos pacotes mais raros, sendo estes pedidos primeiro
		priority_queue = FS_Node_DB.get_rarest_packets(FS_Nodes)

		# Cria um lock para assegurar que cada thread só acede a uma posição
		index = 0
		lock_priority_queue = threading.RLock()

		# Vai buscar os pacotes e guarda-os na base de dados do próprio FS_Node e atualiza os ficheiros do mesmo
		for i in range(threads_per_request):
			thread = threading.Thread(target=get_file_Thread, args=(s, send_lock, FS_Node_DB, FS_Nodes, fileName, priority_queue, index, lock_priority_queue))
			thread.Start()
			threads.append(thread)
		
		# Espera que todas as threads terminem de ir buscar os pacotes
		for thread in threads:
			thread.join()
		
		# Verifica se foi possível transferir o ficheiro completo
		progress = FS_Node_DB.get_number_packets_completed(fileName)
		if (progress[0]==progress[1]):
			write_lock.acquire()
			print(f"O ficheiro {fileName} já está completo.")
			write_lock.release()
		else:
			write_lock.acquire()
			print(f"Apenas foi possível obter {progress[0]} pacotes dos {progress[1]} pacotes totais.")
			write_lock.release()

	else:
		write_lock.acquire()
		print(f"Nenhum FS_Node possuí o ficheiro {fileName}.")
		write_lock.release()


"""
Função responsável por fazer parsing do pedido do utilizador e executar o pedido correspondente. Caso o pedido seja
"ls" então a thread imprime no ecrã os nomes de todos os ficheiros completos e incompletos que o FS_Node possuí. Caso
o utilizador acrscente a flag -c, imprime apenas o nome dos ficheiros completos e se a flag acrescentada for -i,
imprime apenas os incompletos. Se o utilizador fizer o pedido "check" então será imprimida a percentagem de um ficheiro
à sua escolha ou então de todos os ficheiros, se pretender. Por fim, caso o pedido seja "get" a função chamará outra
função responsável por obter o ficheiro pretendido.

Importante salientar que as escritas no ecrã são controladas por um lock de forma a não misturar escritas.
"""
def requests_handler_thread(s, send_lock, write_lock, threads_per_request, FS_Node_DB, user_input):
	if (command := user_input.lower().strip().split())[0] == "get" and len(command)==2:
		fileName = command[1]
		downloadFile(s, send_lock, write_lock, threads_per_request, FS_Node_DB, fileName)
	elif (user_input.lower().strip()=="ls"):
		name_files = FS_Node_DB.get_files_names(0)
		write_lock.acquire()
		for name in name_files:
			print(name + " ", end="")
		write_lock.release()
	elif (user_input.lower().strip()=="ls -c"):
		name_files = FS_Node_DB.get_files_names(1)
		write_lock.acquire()
		for name in name_files:
			print(name + " ", end="")
		write_lock.release()
	elif (user_input.lower().strip()=="ls -i"):
		name_files = FS_Node_DB.get_files_names(2)
		write_lock.acquire()
		for name in name_files:
			print(name + " ", end="")
		write_lock.release()
	elif (command := user_input.lower().strip().split())[0] == "check" and len(command)==2:

		# Função que imprime no terminal uma representação do estado da transferência de um ficheiro
		if command[1]=="-all":
			name_files = FS_Node_DB.get_files_names(0)
			write_lock.acquire()
			for name in name_files:
				progress = FS_Node_DB.get_number_packets_completed(name)
				filled_blocks = int((progress[0] / progress[1]) * 50)
				empty_blocks = 50 - filled_blocks
				progress_bar = "█" * filled_blocks + "░" * empty_blocks
				print(f"[{progress_bar}] {progress} out of {progress[1]}")
			write_lock.release()
		else:
			progress = FS_Node_DB.get_number_packets_completed(command[1])
			if (progress==-1):
				write_lock.acquire()
				print("File does not exist.")
				write_lock.release()
			else:
				filled_blocks = int((progress[0] / progress[1]) * 50)
				empty_blocks = 50 - filled_blocks
				progress_bar = "█" * filled_blocks + "░" * empty_blocks
				write_lock.acquire()
				print(f"[{progress_bar}] {progress} out of {progress[1]}")
				write_lock.release()
	else:

		# Executa caso o comando introduzido pelo utilizador não exista, informando o mesmo que o comando não existe
		write_lock.acquire()
		print("Command not found.")
		write_lock.release()


"""
Função responsável por receber os pedidos de pacotes de outros FS_Nodes e responder com o pacote pedido ou uma mensagem
vazia a indicar que já não possuí o pacote pedido.
"""
def listener_thread():


	ola = 0
	# Completar

	# Se não tiver o pacote manda uma mensagem vazia apenas com o tamanho da mesma, se tiver manda apenas o pacote com o tamanho da mesma antes






def Main(threads_per_request, dir, path_to_metadata="FS_Node_Files/metadata.txt"):

	# Lock para impedir duas escritas consecutivas no mesmo socket buffer
	send_lock = threading.RLock()

	# Lock para escrever no terminal
	write_lock = threading.RLock()

	# Inícia a base de dados do FS_Node
	FS_Node_DB = FS_Node_DataBase()
	
	# Estabelece uma conexão TCP entre o FS_Node
	server_ip = '127.0.0.1'
	server_port = 9090
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect((server_ip, server_port))

	# Popula a base de dados do FS_Node com os ficheiros que este possuí
	initial_files = fetch_files(dir,path_to_metadata)
	FS_Node_DB.add_files(initial_files)

	# Envia para o FS_Tracker os ficheiros ou partes de ficheiros que possuí
	Message_Protocols.send_message(s, send_lock, initial_files)

	# Sinal ativado quando o clinte termina o programa premindo ctrl+c
	signal.signal(signal.SIGINT, lambda signum, frame: signal_handler(signum, frame, dir, FS_Node_DB))

	# Criar thread que trata dos pedidos de outros FS_Nodes
	thread = threading.Thread(target=listener_thread, args=())
	thread.Start()

	while True:	
		user_input = input("FS_Node > ")

		if (user_input.lower().strip()!="exit"):

			# Cria uma thread que será responsável por executar um pedido do utilizador
			thread = threading.Thread(target=requests_handler_thread, args=(s, send_lock, write_lock, threads_per_request, FS_Node_DB, user_input))
			thread.Start()
		else:

			# Guarda os dados dos ficheiros do FS_Node num ficheiro de metadados
			write_MTDados(dir, FS_Node_DB)

			break

	s.close()


if __name__ == '__main__':
	Main()
