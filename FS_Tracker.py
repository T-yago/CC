import socket
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

def request_Thread(c, addr, FS_Tracker, message, send_lock):
    if (message[0]==0):
        response = FS_Tracker.get_file_owners(message[1])
        send_message(c, send_lock, response, False)
    else if (message[0]==1):
        response = FS_Tracker.update_information(addr, message[1])
        send_message(c, send_lock, "UPDATED.", False)

"""
Função responsável por gerir os pedidos e as respostas de um FS_Node, criando uma thread por cada mensagem completa
recebida pelo FS_Tracker de um determinado FS_Node.
"""
def client_thread(c, addr, FS_Tracker, RW_Lock):

    send_lock = threading.Lock()
    database_lock = threading.Lock()

    # Recebe os ficheiros que o FS_Node possuí. Devolve -1 caso o cliente tenha fechado a conexão.
    message = recieve_message(c, 0)

    if (message!=-1) {
        FS_Tracker.handle_data(addr, message, RW_Lock)

        while True:

            message = recieve_message(c, 1)

            if (message!=-1):
                thread = threading.Thread(target=request_Thread, args=(c, addr, FS_Tracker, message, send_lock))
                thread.Start()
            else:
                break
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

    # Inícia o protocolo do servidor e o lock associado ao mesmo
    FS_Tracker = FS_Tracker()
    RW_Lock = ReentrantRWLock()

    # a forever loop until client wants to exit
    while True:

        # establish connection with client
        c, addr = s.accept()

        """
        # lock acquired by client
        print_lock.acquire()
        print('Connected to :', addr[0], ':', addr[1])
        print_lock.release()
        """

        # Cria uma thread que será responsável por gerrir a comunicação entre o FS_Tracker e um FS_Node
        thread = threading.Thread(target=client_thread, args=(c, addr, FS_Tracker, RW_Lock))
        thread.Start()
    
    s.close()



if __name__ == '__main__':
	Main()

