"""
Ficheiro que executa o FS_Tracker.
"""

import socket
import sys
import threading
import Message_Protocols
from FS_Track_DataBase import FS_Tracker_DataBase



"""
Thread responsável por adcionar ou atualizar os dados guardados no FS_Tracker de cada FS_Node. Cada FS_Node
tem apenas associado uma thread destas, para assegurar que os dados são guardados cronologicamente, ou seja,
assegura que as informações de um FS_Node enviadas primeiro são guardadas antes das que são enviadas depois.
Esta estratégia serve para prevenir, casos como, em que o FS_Node decidiu enviar um pacote a atualizar um
ficheiro e de seguida um a apagar todos os ficheiros. Se usassemos múltiplas threads para escrever em memória,
poderia acontecer de executar primeiro a thread que apagou os dados todos e só depois a que atualizava o
ficheiro. O resultado disto seria errado, pois em vez do FS_Tracker ficar sem informações do FS_Node, este
iria ter guardado informações do FS_Node relativamente a ficheiros que este na realidade não contem.

Importante realçar que a thread lê de uma lista que é partilhada com as threads que realizam os pedidos.
Desta forma, as threads que realizam os pedidos escrevem para esta lista partilhada caso o pedido do FS_Node
envolva uma escrita na memória.
"""
def thread_for_store(FS_Tracker_DB, data_to_store_lock, condition, data_to_store, addr):

    while True:
        with data_to_store_lock:
            while (len(data_to_store) == 0):
                condition.wait()
            message = data_to_store.pop(0)
        if (message[0]==1):
            FS_Tracker_DB.update_information(addr, message[1])
        elif (message[0]==2):
            number_packets_file = FS_Tracker_DB.get_size_file(message[1][0])
            packet_update_index = 1 << number_packets_file - message[1][1] - 1
            FS_Tracker_DB.update_information(addr, [[message[1][0], number_packets_file, packet_update_index]])
        elif (message[0]==3):
            number_packets_file = FS_Tracker_DB.get_size_file(message[1][0])
            FS_Tracker_DB.update_information(addr, [[message[1][0], number_packets_file, message[1][1]]])


"""
Para cada conexão estabelecida entre o FS_Tracker e um cliente, é criada uma nova thread que é responsável
por gerir essa conexão, nomeadamente converter as mensagens de binário para os seus tipos de dados correspondentes
e invocar as funções necessárias para satisfazer os pedidos do cliente. Estas são ainda responsáveis, por
assegurar que o cliente faz um pedido que o servidor pode satisfazer, sendo estes 2 pedidos possíveis, pedir
um ficheiro (0) e atualizar dados de um ficheiro (1).

No caso de o cliente fazer 2 pedidos ou mais consecutivamente, chegando a primeira mensagem e outras ou partes
de outras ao buffer do socket antes que a thread as consiga ler, é importante termos em atenção o tamanho das
mensagens, assegurando que não misturamos mensagens. Desta forma, os primeiros 4 bytes de todas as mensagens
correspondem sempre a 1 inteiro de 4 bytes, que indica o tamanho da mensagem.
"""
def request_Thread(c, FS_Tracker_DB, message, send_lock, data_to_store, data_to_store_lock, condition):
    if (message[0]==0):
        response = FS_Tracker_DB.get_file_owners(message[1])
        Message_Protocols.send_message_TCP(c, send_lock, response, False)
    else:
        with data_to_store_lock:
            data_to_store.append(message)
            condition.notify()


"""
Função responsável por gerir os pedidos e as respostas de um FS_Node, criando uma thread por cada mensagem completa
recebida pelo FS_Tracker de um determinado FS_Node.
"""
def client_thread(c, addr, FS_Tracker_DB):

    # Lock para impedir duas escritas consecutivas no mesmo socket buffer
    send_lock = threading.Lock()

    # Inicía a thread que será responsável por escrever as mensagens na base de dados do FS_Tracker e as variáveis necessárias
    data_to_store = []
    data_to_store_lock = threading.Lock()
    condition = threading.Condition(data_to_store_lock)
    thread = threading.Thread(target=thread_for_store, args=(FS_Tracker_DB, data_to_store_lock, condition, data_to_store, addr))
    thread.start()

    while True:

        message = Message_Protocols.receive_message_TCP(c, True)

        if (message!=-1):
            thread = threading.Thread(target=request_Thread, args=(c, FS_Tracker_DB, message, send_lock, data_to_store, data_to_store_lock, condition))
            thread.start()
        else:
            FS_Tracker_DB.remove_FS_node(addr)
            break

	# Fechar a conexão
    c.close()


def Main():

    # Vai buscar os argumentos fornecidos pelo cliente
    if len(sys.argv) != 3:
        print("Argumentos introduzidos errados.")
        print("Formato Correto: python3 FS_Tracker.py Tracker_Name Tracker_Port")
        return

    host_Name, port = sys.argv[1:]

    FS_Tracker_IP = socket.gethostbyname(host_Name)
    Tracker_Port = int(port)

    # Cria o socket na porta correspondente para aceitar conexões de FS_Nodes
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((FS_Tracker_IP, Tracker_Port))
    s.listen(5)

    # Inicía a base de dados do FS_Tracker
    FS_Tracker_DB = FS_Tracker_DataBase()

    while True:

        # Espera que os clientes se conectem
        c, addr = s.accept()
        addr[1], _, _ = socket.gethostbyaddr(addr[1])

        # Cria uma thread que será responsável por gerrir a comunicação entre o FS_Tracker e um FS_Node
        thread = threading.Thread(target=client_thread, args=(c, addr, FS_Tracker_DB))
        thread.start()
    
    s.close()


if __name__ == '__main__':
	Main()

