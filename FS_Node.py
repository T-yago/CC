import socket


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
