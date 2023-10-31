import socket

# Variável com o tamanho de cada bloco (este tamanho é fixo, 1 KB) 
block_size = 1024 

def write_block_File (N_block, message, File_Written):

	# Calcular a posição de escrita para este bloco
    start_position = N_block * block_size
    
	# Abrir o arquivo em modo de escrita binária
    with open(File_Written, 'rb+') as file:
        # Mover para a posição correta no arquivo
        file.seek(start_position)

        # Escrever o conteúdo do bloco
        file.write(message.encode())
        
    
def read_block_file(N_block, file_path):
    try:
        with open(file_path, 'rb') as file:

            file.seek((N_block - 1) * block_size)  # Posicionar o ponteiro de arquivo no início do bloco desejado

            # Ler o bloco
            block_data = file.read(block_size)

            return block_data
    except FileNotFoundError:
        return None  # O arquivo não foi encontrado
    except Exception as e:
        print("Erro ao ler o bloco:", e)
        return None
    
	



def Main():
	
	# Lock para impedir duas escritas consecutivas no mesmo socket buffer
	send_lock = threading.Lock()
	FS_Node = FS_Node()
	
	# Cria uma conexão TCP entre o FS_Node e o servidor (TCP) e informa o servidor dos ficheiros que possuí
	server_ip = '127.0.0.1'
	server_port = 12345

	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect((server_ip, server_port))
	initial_files = FS_Node.get_files()
	send_message(s, send_lock, initial_files)









	while True:

		


		
		# close the connection
		s.close()

if __name__ == '__main__':
	Main()
