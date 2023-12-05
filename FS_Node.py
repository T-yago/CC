"""
Ficheiro que executa o FS_Node.
"""


import math
import socket
import threading
import signal
import sys
import os
import time
import FS_Node_DataBase
import Message_Protocols
import IntegerInstance



# Tamanho de cada pacote de um ficheiro
PACKET_SIZE = 100


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
def signal_handler(sig, frame, Node_IP, FS_Node_DB):
	write_MTDados(Node_IP, FS_Node_DB)
	
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
def write_MTDados(path_to_metadata, FS_Node_DB):

	with open(path_to_metadata, "w") as file:
		for (name, n_packets, packets) in FS_Node_DB.get_files():
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
    current_index = 0

    with open(meta_file, "r") as file:
        lines = file.readlines()
	
    while current_index < len(lines):
        name = lines[current_index].strip()
        n_packets = int(lines[current_index + 1])
        packets = int(lines[current_index + 2])
        metadata.append([name, n_packets, packets])

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
def fetch_files(files_path, path_to_metadata):

	files = []

	# Vê se existe ficheiro de metadados
	if os.path.exists(files_path) and os.path.isdir(files_path):

			if os.path.isfile(path_to_metadata):

				# se existe, popula o dicionário
				metadata = get_file_metadata(path_to_metadata)
				for name, n_packets, packets in metadata:
					files.append([name, n_packets, packets])
			
			# vê se existem ficheiros que não estão no ficheiro de metadados
			for file_name in os.listdir(files_path):
				file_path = os.path.join(files_path, file_name)
				if os.path.isfile(file_path) and file_name not in [file_names[0] for file_names in files]:
					# Se existirem, adiciona-os ao dicionário, assumindo que estão completos
					file_size = os.path.getsize(file_path)
					files.append([file_name, math.ceil(file_size / PACKET_SIZE), -1])

	else:
		print(f"Folder '{files_path}' does not exist.")

	return files


"""
Função que retorna uma lista ordenada por ordem descrescente de prioridade a quem o FS_Node deve pedir o
pacote que pretende. Esta percorre a lista que possuí a informação sobre os FS_Nodes que não possuem e que
possuem o pacote que FS_Node a partir da posição central e expandindo em redor da mesma. Este algoritmo, serve
para tentar escolher o FS_Node que se encontra a responder a uma menor quantidade de pedidos referentes ao
ficheiro que nós pretendemos. Assim, tentamos não sobrecarregar nenhum FS_Node.
"""
def FS_Nodes_ask_order(list_FS_Nodes_With_Packet):
	
	#Adiciona o primeiro ip da lista no início da lista
	FS_Nodes_ask_order = [list_FS_Nodes_With_Packet[0]]

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
	
	else:

		# Percorre a lista a partir do ponto central, adicionando à lista os endereços IP
		for i in range(1, index_start_point, 1):

			# Verifica se contêm um endereço ip nos índices i posições deslocados da posição inicial
			if ((ip := list_FS_Nodes_With_Packet[index_start_point-i])!=0):
				FS_Nodes_ask_order.append(ip)
			if ((ip := list_FS_Nodes_With_Packet[index_start_point+i])!=0):
				FS_Nodes_ask_order.append(ip)

		if ((ip := list_FS_Nodes_With_Packet[size_list-1])!=0):
				FS_Nodes_ask_order.append(ip)
	
	return FS_Nodes_ask_order



"""
Função que retorna uma lista em que cada posição corresponde a um FS_Node, estando a 0 caso o FS_Node não possua o
pacote pretendido e contendo o endereço IP caso contenha o pacote pretendido.
"""
def FS_Nodes_with_packet(FS_Nodes, packet_to_check):

	# Lista com os FS_Nodes que possuem o pacote pretendido, estando a posição igual ao IP caso tenha o pacote e a 0 caso não tenha
	list = [0] * len(FS_Nodes[1:])

	# Percorre todos os FS_Nodes à procura dos que têm o ficheiro
	index = 0
	for (address, packets) in FS_Nodes[1:]:

		# Verifica se o FS_Node tem o pacote
		binary_to_compare = 1 << FS_Nodes[0] - packet_to_check - 1
		if (binary_to_compare & packets > 0):
			list[index] = address
		else:
			list[index] = 0
		index += 1
	
	return list


"""
Função responsável por converter os elementos da lista de FS_Nodes enviada pelo Tracker aquando de um pedido
de um ficheiro, do formato ["172.0.0.1", ["168.98.2.1", 7123]] para [["172.0.0.1", 8192], ["168.98.2.1", 7123]]
"""
def convert_complete_FS_Nodes(FS_Nodes):
    new_list = [FS_Nodes[0]]

    complete_value = pow(2, FS_Nodes[0]) - 1
    for file in FS_Nodes[1:]:
        if not isinstance(file[0], list):
            new_list.append([tuple(file), complete_value])
        else:
            new_list.append([tuple[file[0]], file[1]])
	
    return new_list


"""
Função responsável por obter os pacotes do ficheiro. Esta começa por escolher um pacote para obter, determina
qual o melhor FS_Node a quem pedir o pacote, tenta obter o pacote e, por fim, atualiza a informação da base de
dados do FS_Node e atualiza o ficheiro correspondente ao pacote.

Importnate salientar, que caso o timeout de espera para obter uma resposta de outro FS_Node seja ultrapassado 3
vezes, é escolhido outro FS_Node a quem pedir o pacote. Caso não consiga obter o pacote de nenhum FS_Node então
passa para outro pacote e no fim da transferência do ficheiro, avisa o utilizador que não foi possível obter
o ficheiro completo.
"""
def get_file_Thread(s, send_lock_TCP, send_queue_UDP, send_queue_UDP_lock, send_queue_UDP_condition, replies_Dic, replies_Dic_lock, FS_Node_DB, FS_Nodes, files_path, fileName, priority_queue, index, lock_priority_queue):
	
	# Obtem um dos pacotes do ficheiro
	lock_priority_queue.acquire()
	while (index.getInt()<len(priority_queue)):
		packet_to_check = priority_queue[index.getInt()]
		meu_Index = index.getInt()
		index.incrementInteger()
		lock_priority_queue.release()

		# Verifica se o FS_Node já possuí esse pacote
		if (not FS_Node_DB.check_packet_file(fileName, packet_to_check)):
			
			packet = None
			# Verifica se o pacote já não está em cache
			if not ((response := replies_Dic.get(fileName + str(packet_to_check)))!=None and response[3]!=None and len(response[3])>0):

				# Determina quantos FS_Nodes e que FS_Nodes possuem o pacote
				list_FS_Nodes_With_Packet = FS_Nodes_with_packet(FS_Nodes, packet_to_check)

				# Determina a que FS_Nodes irá pedir os pacotes
				FS_Nodes_ask_order_list = FS_Nodes_ask_order(list_FS_Nodes_With_Packet)

				# Cria um lock com uma condição associada para ser alertado de quando a resposta ao seu pedido chegar
				wake_me_lock = threading.Lock()
				wake_me_lock_condition = threading.Condition(wake_me_lock)

				# Pede o pacote a outros FS_Nodes
				found = False
				for FS_Node_address in FS_Nodes_ask_order_list:

					# Verifica se o pacote já foi encontrado
					if found:
						break

					attempts = 0
					while attempts < 3:
						# Insere o pedido de ficheiro na queue de pedidos, que serão tratados por outras thread
						
						# Caso a thread não seja notificada, esta espera apenas 2 segundos
						wake_me_lock.acquire()
						send_queue_UDP_lock.acquire()
						send_queue_UDP.append([0, wake_me_lock, wake_me_lock_condition, [fileName, packet_to_check, FS_Node_address]])
						send_queue_UDP_condition.notify()
						send_queue_UDP_lock.release()
						wake_me_lock_condition.wait(2.0)
						wake_me_lock.release()
						
						# Verifica se já tem a resposta ao seu pedido no dicionário de respostas a pedidos
						if (response := replies_Dic.get(fileName + str(packet_to_check)))!=None and response[3]!=None:

							if (len(response[3])>0):
								packet = response[3]
								found = True
							break
						else:
							attempts += 1
			else:
				response[0] = time.time()
				packet = response[3]

			if packet!=None:
				# Cria o ficheiro se ele não existir
				if not os.path.exists(files_path + fileName):
					with open(files_path + fileName, 'w') as new_file:
						pass

				# Escreve o pacote recebido para o ficheiro correspondente
				with open(files_path + fileName, 'r+') as file:
					file.seek((meu_Index)*PACKET_SIZE)
					file.write(packet)
				
				# Guarda o pacote na base de dados do FS_Node
				FS_Node_DB.update_packet(fileName, meu_Index)
				
				# Informa o FS_Tracker da atualização
				message = (fileName, meu_Index)
				Message_Protocols.send_message_TCP(s, send_lock_TCP, message, True, 2)

		lock_priority_queue.acquire()
	
	lock_priority_queue.release()


"""
Função responsável por fazer download de um ficheiro. Começa por pedir ao FS_Tracker os FS_Nodes que possuem pacotes
do ficheiro que pretende obter e depois cria uma lista ordenada por ordem crescente dos pacotes mais comuns na rede.
Por fim, cria X threads responsáveis por fazer o download do ficheiro.
"""
def downloadFile(s, send_lock_TCP, send_queue_UDP, send_queue_UDP_lock, send_queue_UDP_condition, replies_Dic, replies_Dic_lock, write_lock, threads_per_request, FS_Node_DB, files_path, fileName):

	# Pede ao FS_Tracker os FS_Nodes que possuem informação sobre o ficheiro
	send_lock_TCP.acquire()
	Message_Protocols.send_message_TCP(s, send_lock_TCP, fileName, True, 0)
	FS_Nodes = Message_Protocols.receive_message_TCP(s, False)
	send_lock_TCP.release()

	# Cria uma lista para guardar as threads
	threads = []

	if len(FS_Nodes)>0:
		# Adiciona o ficheiro à lista de ficheiros, mas com 0 pacotes
		FS_Node_DB.add_files([[fileName, FS_Nodes[0], 0]])

		# Converte as posições onde apenas tem um endereço IP, pois o ficheiro está completo, para um tuplo do mesmo formato se o ficheiro fosse completo (IP_address, packets)
		FS_Nodes = convert_complete_FS_Nodes(FS_Nodes)

		# Organiza a informação recebida pelos pacotes mais raros, sendo estes pedidos primeiro
		priority_queue = FS_Node_DB.get_rarest_packets(FS_Nodes)

		# Cria um lock para assegurar que cada thread só acede a uma posição
		index = IntegerInstance.IntegerInstance(0)
		lock_priority_queue = threading.RLock()

		# Vai buscar os pacotes e guarda-os na base de dados do próprio FS_Node e atualiza os ficheiros do mesmo
		for i in range(threads_per_request):
			thread = threading.Thread(target=get_file_Thread, args=(s, send_lock_TCP, send_queue_UDP, send_queue_UDP_lock, send_queue_UDP_condition, replies_Dic, replies_Dic_lock, FS_Node_DB, FS_Nodes, files_path, fileName, priority_queue, index, lock_priority_queue))
			thread.start()
			threads.append(thread)
		
		# Espera que todas as threads terminem de ir buscar os pacotes
		for thread in threads:
			thread.join()
		
		
		# Verifica se foi possível transferir o ficheiro completo
		progress = FS_Node_DB.get_number_packets_completed(fileName)
		if progress!=-1:
			if (progress[0]==progress[1]):
				write_lock.acquire()
				print(f"O ficheiro {fileName} já está completo.")
				write_lock.release()
			else:
				write_lock.acquire()
				if progress[0] == 0:
					FS_Node_DB.remove_file(fileName)
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
def requests_handler_thread(s, send_lock_TCP, send_queue_UDP, send_queue_UDP_lock, send_queue_UDP_condition, replies_Dic, replies_Dic_lock, write_lock, threads_per_request, FS_Node_DB, files_path, user_input):
	if (command := user_input.lower().strip().split())[0] == "get" and len(command)==2:
		fileName = command[1]
		downloadFile(s, send_lock_TCP, send_queue_UDP, send_queue_UDP_lock, send_queue_UDP_condition, replies_Dic, replies_Dic_lock, write_lock, threads_per_request, FS_Node_DB, files_path, fileName)
	elif (user_input.lower().strip()=="ls"):
		name_files = FS_Node_DB.get_files_names(0)
		write_lock.acquire()
		for name in name_files:
			print(name + " ", end="")
		print("")
		write_lock.release()
	elif (user_input.lower().strip()=="ls -c"):
		name_files = FS_Node_DB.get_files_names(1)
		write_lock.acquire()
		for name in name_files:
			print(name + " ", end="")
		print("")
		write_lock.release()
	elif (user_input.lower().strip()=="ls -i"):
		name_files = FS_Node_DB.get_files_names(2)
		write_lock.acquire()
		for name in name_files:
			print(name + " ", end="")
		print("")
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
				print(f"{name} -> [{progress_bar}] {progress} out of {progress[1]}")
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
				print(f"{command[1]} -> [{progress_bar}] {progress} out of {progress[1]}")
				write_lock.release()
	elif (command := user_input.lower().strip().split())[0] == "delete" and ((command[1] == "-all" and len(command)==2) or (command[1] == "-f" and len(command)==3) or (command[1] == "-p" and len(command)==4)):
		if command[1]=="-all":
			files = FS_Node_DB.get_files()
			for file in files:
				packets = FS_Node_DB.get_packets_file(file)
				Message_Protocols.send_message_TCP(s, send_lock_TCP, [file, packets], True, 3)
				FS_Node_DB.remove_file(file)
				os.remove(files_path + file)
		elif command[1] == "-f":
			file = command[2]
			packets = FS_Node_DB.get_packets_file(file)
			if packets!=None:
				Message_Protocols.send_message_TCP(s, send_lock_TCP, [file, packets], True, 3)
				FS_Node_DB.remove_file(file)
				os.remove(files_path + file)
			else:
				print("You don't have that file.")
		elif command[1] == "-p":
			file = command[2]
			packet = int(command[3])
			if packet < FS_Node_DB.get_size_file(file):
				if (FS_Node_DB.check_packet_file(file, packet)):
					FS_Node_DB.update_packet(file, packet)
					Message_Protocols.send_message_TCP(s, send_lock_TCP, [file, packet], True, 2)

					# Apaga o pacote do ficheiro respetivo
					with open(files_path + file, 'r+b') as file:
						file.seek(packet * PACKET_SIZE)
						file.write(b'\0' * PACKET_SIZE)
				else:
					print("You don't have that packet.")
			else:
				print("You inserted an invalid packet index.")
		else:
			print ("Invalid flag for the packet you chose.")
	elif (command := user_input.lower().strip()) == "load new files":

		# Verifica se o caminho passado existe e é uma diretoria
		if os.path.exists(files_path) and os.path.isdir(files_path):

			# Percorre os ficheiros à procura de novos
			files_owned = FS_Node_DB.get_files_names(0)
			for file_name in os.listdir(files_path):
				file_path = os.path.join(files_path, file_name)
				if os.path.isfile(file_path) and file_name not in files_owned:
					file_size = os.path.getsize(file_path)
					n_packets_file = math.ceil(file_size / PACKET_SIZE)
					FS_Node_DB.add_files([[file_name, n_packets_file, -1]])
					Message_Protocols.send_message_TCP(s, send_lock_TCP, [[file_name, n_packets_file, -1]], True, 1)
	else:

		# Executa caso o comando introduzido pelo utilizador não exista, informando o mesmo que o comando não existe
		write_lock.acquire()
		print("Command not found.")
		write_lock.release()


"""

"""
def UDP_listener_thread(socket_UDP, send_queue_UDP, send_queue_UDP_lock, send_queue_UDP_condition, replies_Dic, files_path):

	# Recebe os pacotes pedidos e recebe ainda pedidos de pacotes de outros FS_Nodes
	while (True):

		# Recebe um pedido
		message = Message_Protocols.receive_message_UDP(socket_UDP)

		# Verifica se é um pedido de pacote ou uma resposta a um pedido de pacote (0 se for um pedido, 1 se for uma resposta)
		if message[0]==0:
			destiny, fileName, packet_index = message[1:]
		
			# Verifica se o pacote pedido está no dicionário de pacotes
			if (packet := replies_Dic.get(files_path + fileName))!=None and packet[3]!=None and len(packet[3])>0:

				# Atualiza o timestamp associado ao pacote
				packet[0] = time.time()
				
				# Adiciona o pacote à lista de mensagens a enviar para outros FS_Nodes
				send_queue_UDP_lock.acquire()
				send_queue_UDP.append([1, fileName, packet_index, packet, destiny])
				send_queue_UDP_condition.notify()
				send_queue_UDP_lock.release()

			# Verifica se o ficheiro existe
			elif (os.path.exists(files_path + fileName)):
				
				# Lê um pacote de um ficheiro
				with open(files_path + fileName, 'rb') as file:
					file.seek(packet_index * PACKET_SIZE)
					packet = file.read(PACKET_SIZE).decode('utf-8', errors='replace')
				
				# Adiciona o pacote à lista de mensagens a enviar para outros FS_Nodes
				send_queue_UDP_lock.acquire()
				send_queue_UDP.append([1, fileName, packet_index, packet, destiny])
				send_queue_UDP_condition.notify()
				send_queue_UDP_lock.release()

		elif message[0]==1:

			fileName_packetNumber, data = message[2:]

			# Atualiza o dicionário de pacotes recebidos e acorda a thread que estava à espera do pacote correspondente
			value = replies_Dic.get(fileName_packetNumber)
			
			# Verifica se o pacote não foi recebido anteriormente
			if (value!=None and (value[3]==None or len(value[3])==0)):
				value[0] = time.time()
				value[3] = data
			
			# Avisa a thread caso esta já não tenha sido avisada
			value[1].acquire()
			value[2].notify()
			value[1].release()


"""
Tipo 0 -> [0, RWLock, RWLock_Condition, [fileName, packet_to_check, FS_Node_address]]
Tipo 1 -> [1, fileName, packet_index, packet_data, destiny]
"""
def UDP_sender_thread(socket_UDP, my_address, send_queue_UDP, send_queue_UDP_lock, send_queue_UDP_condition, replies_Dic, replies_Dic_lock):

	# Envia os pedidos de pacotes e as respostas a pedidos de pacotes de outros FS_Nodes
	while True:
		send_queue_UDP_lock.acquire()
		while (len(send_queue_UDP) == 0):
			send_queue_UDP_condition.wait()
		message = send_queue_UDP.pop(0)
		send_queue_UDP_lock.release()
		
		# Verifica qual o tipo de mensagem
		if message[0] == 0:
			timestamp = time.time()

			# Verifica se a entrada correspondente ao pacote já está no dicionário
			fileName = message[3][0]
			packet = message[3][1]
			destiny = message[3][2]
			entry = fileName + str(packet)
			response = None
			if (response := replies_Dic.get(entry))==None:

				replies_Dic_lock.acquire()
				replies_Dic[entry] =  [timestamp, message[1], message[2], None]
				replies_Dic_lock.release()
				Message_Protocols.send_message_UDP(0, socket_UDP, fileName, packet, None, destiny)
			elif (response[3]==None):

				# Verifica se o lock e condition associada à entrada é o da thread que está a pedir o pacote
				if not (message[1] is response[1]):
					response[1] = message[1]
					response[2] = message[2]
				response[0] = timestamp
				Message_Protocols.send_message_UDP(0, socket_UDP, fileName, packet, None, destiny)
		
		elif message[0] == 1:
			Message_Protocols.send_message_UDP(1, socket_UDP, message[1], message[2], message[3], message[4])


"""
Thread responsável por fazer a limpeza das entradas da cache quando estas já existem há mais de X tempo
"""
def Cache_cleaner_thread(replies_Dic, expire_time):

	while True:
		time.sleep(20)

		# Determina o timestamp atual
		timestamp = time.time()

		# Percorre todas as entradas do dicionário e remove as que já existem há mais de X tempo
		for key, info in replies_Dic.items():
			if ((timestamp - info[0]) > expire_time):
				del replies_Dic[key]


"""

"""
def Main():

	# Vai buscar os argumentos fornecidos pelo cliente
	if len(sys.argv) != 9:
		print("Argumentos introduzidos errados.")
		print("Formato Correto: python3 FS_Node.py Node_IP Node_Port Tracker_IP Tracker_Port threads_per_request files_path metadados_path")
		return

	Node_IP, Node_Port, Tracker_IP, Tracker_Port, threads_per_request, files_path, metadados_path, expire_time = sys.argv[1:]

	# Associa a pasta dos ficheiros do FS_Node correspondente
	files_path += Node_Port + "/"

	# Caminho para os metadados caso estes existam
	path_to_metadata = metadados_path + "metadata" + Node_IP + Node_Port + ".txt"

	# Lock para escrever no terminal
	write_lock = threading.RLock()

	# Inícia a base de dados do FS_Node
	FS_Node_DB = FS_Node_DataBase.FS_Node_DataBase()

	Node_Port = int(Node_Port)
	Tracker_Port = int(Tracker_Port)
	threads_per_request = int(threads_per_request)
	# Estabelece uma conexão TCP entre o FS_Node e o FS_Tracker e cria um lock para controlar as leituras e escritas no socket TCP
	server_ip = Tracker_IP
	server_port = Tracker_Port
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.bind((Node_IP, Node_Port))
	s.connect((server_ip, server_port))
	send_lock_TCP = threading.RLock()

	# Cria o socket UDP
	socket_UDP = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	my_address = (Node_IP, Node_Port)
	socket_UDP.bind(my_address)

	# Cria a fila de pedidos a serem enviados para o socket UDP, os locks associados, o dicionário de respostas a pedidos e o lock associado ao dicionário
	send_queue_UDP = []
	send_queue_UDP_lock = threading.Lock()
	send_queue_UDP_condition = threading.Condition(send_queue_UDP_lock)
	replies_Dic = {}
	replies_Dic_lock = threading.Lock()

	# Cria as threads que serão responsáveis por gerir o socket UDP, uma para enviar, outra para receber dados e outra para limpar a cache de pacotes
	thread = threading.Thread(target=UDP_sender_thread, args=(socket_UDP, my_address, send_queue_UDP, send_queue_UDP_lock, send_queue_UDP_condition, replies_Dic, replies_Dic_lock))
	thread.start()
	thread = threading.Thread(target=UDP_listener_thread, args=(socket_UDP, send_queue_UDP, send_queue_UDP_lock, send_queue_UDP_condition, replies_Dic, files_path))
	thread.start()
	thread = threading.Thread(target=Cache_cleaner_thread, args=(replies_Dic, expire_time))
	thread.start()

	# Popula a base de dados do FS_Node com os ficheiros que este possuí
	initial_files = fetch_files(files_path, path_to_metadata)
	FS_Node_DB.add_files(initial_files)

	# Envia para o FS_Tracker os ficheiros ou partes de ficheiros que possuí
	Message_Protocols.send_message_TCP(s, send_lock_TCP, initial_files, True, 1)

	# Sinal ativado quando o clinte termina o programa premindo ctrl+c
	signal.signal(signal.SIGINT, lambda signum, frame: signal_handler(signum, frame, path_to_metadata, FS_Node_DB))

	while True:
		user_input = input("FS_Node > ")

		if (user_input.lower().strip()!="exit"):

			# Cria uma thread que será responsável por executar um pedido do utilizador
			thread = threading.Thread(target=requests_handler_thread, args=(s, send_lock_TCP, send_queue_UDP, send_queue_UDP_lock, send_queue_UDP_condition, replies_Dic, replies_Dic_lock, write_lock, threads_per_request, FS_Node_DB, files_path, user_input))
			thread.start()
		else:

			# Guarda os dados dos ficheiros do FS_Node num ficheiro de metadados
			write_MTDados(path_to_metadata, FS_Node_DB)

			break

	s.close()


if __name__ == '__main__':
	Main()
