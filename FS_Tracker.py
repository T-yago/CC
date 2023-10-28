import socket
from _thread import *
import threading
import Message_Protocols

"""
Para cada conexão estabelecida entre o FS_Tracker e um cliente, é criada uma nova thread que é responsável por gerir essa conexão,
nomeadamente converter as mensagens de binário para os seus tipos de dados correspondentes e invocar as funções necessárias para
satisfazer os pedidos do cliente. Estas são ainda responsáveis, por assegurar que o cliente faz um pedido que o servidor pode satisfazer,
sendo estes 2 pedidos possíveis, pedir um ficheiro (0) e atualizar dados de um ficheiro (1). As respostas do servidor para os clientes, são
igualmente convertidas para binário para serem enviadas na rede.

No caso de o cliente fazer 2 pedidos ou mais consecutivamente, chegando a primeira mensagem e outras ou partes de outras ao buffer do socket
antes que a thread as consiga ler, é importante termos em atenção o tamanho das mensagens, assegurando que não misturamos mensagens. Desta forma,
os primeiros 4 bytes de todas as mensagens correspondem sempre a 1 inteiro de 4 bytes, que indica o tamanho da mensagem.
"""
def client_thread(c, addr, FS_Tracker):

    # Recebe os ficheiros que o FS_Node possuí. Devolve -1 caso o cliente tenha fechado a conexão.
    message = recieve_message(c, 0)

    if (message!=-1) {
        FS_Tracker.handle_data(addr, message)

        while True:

            message = recieve_message(c, 1)

            if (message!=-1):
                if (message[0]==0):
                    response = FS_Tracker.get_file_owners(message[1])
                    send_message(c, response, False)
                else if (message[0]==1):
                    response = FS_Tracker.update_information(addr, message[1])
                    send_message(c, "UPDATED.", False)
    }

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
        start_new_thread(client_thread, (c, addr, FS_Tracker))
    s.close()



if __name__ == '__main__':
	Main()

