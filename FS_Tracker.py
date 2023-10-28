import socket
import pickle

# import thread module
from _thread import *
import threading

print_lock = threading.Lock()

# Cliente começa por enviar os ficheiros ou partes de ficheiros que possuí na forma 
def threaded(c, addr, FS_Tracker):

    # Receber ficheiros que o FS_Node possuí
    size_files_FS_Node = pickle.loads(c.recv(4))
    files_FS_Node = pickle.loads(c.recv(size_files_FS_Node))
    FS_Tracker.handle_data(addr, files_FS_Node)

	while True:

		size_data = pickle.loads(c.recv(4))
		if not size_data:
            print_lock.acquire()
			print('Bye '+ addr)
			print_lock.release()

			break


        data_FS_Node = pickle.loads(c.recv(size_data))

        if (data_FS_Node[0].lower()=="get"):
            response = FS_Tracker.get_file_owners(data_FS_Node[1])
            serialized_response = pickle.dumps(response)
            bytes_packet = len(serialized_response)

            c.sendall(serialized_response)
        else if (data_FS_Node[0].lower()=="update"):
            response = FS_Tracker.update_information(addr, data_FS_Node[1])
            """
            c.sendall(pickle.dumps("UPDATED."))
            """

	# Fechar a conexão
	c.close()


def Main():
    host = ""

    # reserve a port on your computer
    # in our case it is 12345 but it
    # can be anything
    port = 12345
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    print("socket binded to port", port)

    # put the socket into listening mode
    s.listen(5)
    print("socket is listening")

    # Inícia o protocolo do servidor
    FS_Tracker = FS_Tracker()

    # a forever loop until client wants to exit
    while True:

        # establish connection with client
        c, addr = s.accept()

        # lock acquired by client
        print_lock.acquire()
        print('Connected to :', addr[0], ':', addr[1])
        print_lock.release()

        # Start a new thread and return its identifier
        start_new_thread(threaded, (c, addr, FS_Tracker))
    s.close()



if __name__ == '__main__':
	Main()

