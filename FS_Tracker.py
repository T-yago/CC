import socket
import threading
import Message_Protocols



"""
Thread responsável por adcionar ou atualizar os dados guardados no FS_Tracker de cada FS_Node. Cada FS_Node tem apenas
associado uma thread destas, para assegurar que os dados são guardados cronologicamente, ou seja, assegura que as informações de um
FS_Node enviadas primeiro são guardadas antes das que são enviadas depois. Esta estratégia serve para prevenir, casos como,
em que o FS_Node decidiu enviar um pacote a atualizar um ficheiro e de seguida um a apagar todos os ficheiros. Se
usassemos múltiplas threads para escrever em memória, poderia acontecer de executar primeiro a thread que apagou os dados todos
e só depois a que atuyalizava o ficheiro. O resultado disto seria errado, pois em vez do FS_Tracker ficar sem informações do
FS_Node, este iria ter guardado informações do FS_Node relativamente a ficheiros que este na realidade não contem.

Importante realçar que a thread lê de uma lista que é partilhada com as threads que realizam os pedidos. Desta forma, as threads
que realizam os pedidos escrevem para esta lista partilhada caso o pedido do FS_Node envolva uma escrita ou caso seja a primeira
interação do FS_Node com o FS_Tracker, sendo necessário executar a função handle_data(), estando identificada com o identificador -1.
Por sua vez, pedidos que envolvam atualizações dos dados do cliente são identificados com o inteiro 1.
"""
def thread_for_store(FS_Tracker, c, condition, data_to_store, addr, send_lock):
    while True:
        with condition:
            if len(data_to_store) == 0:
                condition.wait()
            message = data_to_store.pop(0)
        if (message[0]==-1):
            FS_Tracker.handle_data(addr, message)
        else if (message[0]==1):
            response = FS_Tracker.update_information(addr, message[1])
            send_message(c, send_lock, "UPDATED.", False)

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
def request_Thread(c, addr, FS_Tracker, message, send_lock, data_to_store, condition):
    if (message[0]==0):
        response = FS_Tracker.get_file_owners(message[1])
        send_message(c, send_lock, response, False)
    else if (message[0]==1):
        with condition:
            data_to_store.append(message)
            condition.notify()

"""
Função responsável por gerir os pedidos e as respostas de um FS_Node, criando uma thread por cada mensagem completa
recebida pelo FS_Tracker de um determinado FS_Node.
"""
def client_thread(c, addr, FS_Tracker):

    # Lock para impedir duas escritas consecutivas no mesmo socket buffer
    send_lock = threading.Lock()

    # Inicía a thread que será responsável por escrever as mensagens no servidor e as variáveis necessárias
    data_to_store = []
    lock = threading.Lock()
    condition = threading.Condition(lock)
    thread = threading.Thread(target=thread_for_store, args=(FS_Tracker, c, condition, data_to_store, addr, send_lock))
    thread.Start()

    # Recebe os ficheiros que o FS_Node possuí. Devolve -1 caso o cliente tenha fechado a conexão.
    message = recieve_message(c, 0)

    if (message!=-1) {
        data_to_store.append((-1,message))
        condition.notify()

        while True:

            message = recieve_message(c, 1)

            if (message!=-1):
                thread = threading.Thread(target=request_Thread, args=(c, addr, FS_Tracker, message, send_lock, data_to_store, condition))
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
        thread = threading.Thread(target=client_thread, args=(c, addr, FS_Tracker))
        thread.Start()
    
    s.close()


if __name__ == '__main__':
	Main()

