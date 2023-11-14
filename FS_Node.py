import socket
import threading
import signal
import sys
import os
import FS_Node_DataBase
import Message_Protocols

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
...
...
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
....
"""

def get_file_metadata(meta_file):
	metadata = [] 
	file_index = 0
	current_index = 0

	with open(meta_file, "r") as file:
		lines = file.readlines()

	while current_index < len(lines):
		line = lines[current_index].strip()
		name = line
		n_packets = int(lines[current_index + 1])
		packets = int(lines[current_index + 2])
		metadata[file_index] = [name, n_packets, packets]
		current_index += 3  # Passa para a próxima linha onde pode estar o nome de um ficheiro
		
	return metadata

"""
Função responsável por devolver um dicionário com os ficheiros que o FS_Node possuí.
Primeiro, é verificado se existe um ficheiro de metadados.
Caso exista, então é lido e o dicionário é populado com os dados do ficheiro.
De seguida, verifica-se se existem ficheiros que não estão no ficheiro de metadados.
Caso existam, são adicionados ao dicionário, sendo assumindo que estão completos. 

"""
def fetch_files(self, dir, path_to_metadata):
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
Função responsável por criar uma conexão TCP entre o FS_Node e o servidor (TCP), com verificação de exceção caso não seja possível estabeler a conexão.
"""
def connect_node(server_ip, server_port):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((server_ip, server_port))
        return s
    except socket.error as e:
        print(f"Error connecting to server: {e}")
        return None

def requests_handler_thread(s, FS_Node_DB, user_input):
	if (command := user_input.lower().strip().split())[0] == "get":






		# Escrever o resto do código.







		user = []
	elif (user_input.lower().strip()!="ls"):
		name_files = FS_Node_DB.get_files_names(0)
		for name in name_files:
			print(name + " ", end="")
	elif (user_input.lower().strip()!="ls -c"):
		name_files = FS_Node_DB.get_files_names(1)
		for name in name_files:
			print(name + " ", end="")
	elif (user_input.lower().strip()!="ls -i"):
		name_files = FS_Node_DB.get_files_names(2)
		for name in name_files:
			print(name + " ", end="")
	elif (command := user_input.lower().strip().split())[0] == "check":

		# Função que imprime no terminal uma representação do estado da transferência de um ficheiro
		if command[1]=="-all":
			name_files = FS_Node_DB.get_files_names(0)
			for name in name_files:
				progress = FS_Node_DB.get_number_packets_completed(command[1])
				filled_blocks = int((progress[0] / progress[1]) * 50)
				empty_blocks = 50 - filled_blocks
				progress_bar = "█" * filled_blocks + "░" * empty_blocks
				print(f"[{progress_bar}] {progress} out of {progress[1]}")
		else:
			progress = FS_Node_DB.get_number_packets_completed(command[1])
			if (progress==-1):
				print("File does not exist.")
			else:
				filled_blocks = int((progress[0] / progress[1]) * 50)
				empty_blocks = 50 - filled_blocks
				progress_bar = "█" * filled_blocks + "░" * empty_blocks
				print(f"[{progress_bar}] {progress} out of {progress[1]}")

def Main(dir, path_to_metadata="FS_Node_Files/"):

	# Lock para impedir duas escritas consecutivas no mesmo socket buffer
	send_lock = threading.Lock()

	# Inícia a base de dados do servidor e o lock associado ao mesmo
	FS_Node_DB = FS_Node_DataBase()

	FS_Node_DB.load_existent_files("FS_Node_Files/")
	
	# Cria uma conexão TCP entre o FS_Node e o servidor (TCP) e informa o servidor dos ficheiros que possuí
	server_ip = '127.0.0.1'
	server_port = 12345

	s = connect_node(server_ip, server_port)
	initial_files = fetch_files(dir,path_to_metadata)
	FS_Node_DB.add_files(initial_files)

	Message_Protocols.send_message(s, send_lock, initial_files)
		
	# Sinal ativado quando o clinte termina o programa premindo ctrl+c
	signal.signal(signal.SIGINT, lambda signum, frame: signal_handler(signum, frame, dir, FS_Node_DB))


	while True:	
		user_input = input("FS_Node > ")

		if (user_input.lower().strip!="exit"):
			thread = threading.Thread(target=requests_handler_thread, args=(s, FS_Node_DB, user_input))
			thread.Start()
		else:
			# Guarda os dados dos ficheiros do FS_Node num ficheiro de metadados
			write_MTDados(dir, FS_Node_DB)

			break

	s.close()


if __name__ == '__main__':
	Main()
