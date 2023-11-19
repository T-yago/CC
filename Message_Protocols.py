"""
Ficheiro que contem algumas das funções relativas à troca de mensagens entre o FS_Tracker e os FS_Nodes e entre os FS_Nodes.
"""

import json



"""
Função responsável por receber e traduzir mensagens, sendo também responsável por verificar se o FS_Node ou FS_Tracker
não fecharam a conexão quer durante a transmissão de uma mensagem ou quando não estão a transmitir. Importante referir
que o argumento 'mode' refere-se ao tipo de mensagem que esperamos receber. Este é igual a 0 nos casos em que não
estamos à espera de receber um pedido, por exemplo, quando o FS_Node recebe do FS_Tracker a resposta de um pedido de "get fileX".

O loop assegura que lemos a mensagem toda do socket buffer, caso esta ainda não esteja toda no buffer e a thread já
esteja a tentar ler.

Caso a máquina que está a enviar os dados feche a conexão em qualquer momento, a função devolve -1, caso contrário, se
o argumento 'mode' estiver a False devolve os dados sem especificar o tipo de pedido, já se estiver a True, devolve um tuplo
de dois elementos, sendo o primeiro elemento o inteiro identificador do pedido e o segundo os dados.

Importante salientar que caso a mensagem contenha o campo 'mode' este não é contabilizado para o tamanho da mensagem.
"""
def receive_message_TCP(c, mode):
    length_bytes = c.recv(4)
    if not length_bytes:
        return -1
    message_length = int.from_bytes(length_bytes, byteorder='big')

    if (mode==1):
        request_bytes = c.recv(4)
        if not request_bytes:
            return -1
        request = int.from_bytes(request_bytes, byteorder='big')
    
    message = b''
    while len(message) < message_length:
        data = c.recv(message_length - len(message))
        if not data:
            return -1
        message += data

    message = json.loads(message.decode('utf-8'))

    if (mode==1):
        message = (request, message)

    return message


"""
Função responsável por coverter os tipos de dados e estruturas para binário de forma a o FS_Node poder mandar os dados para o
FS_Tracker (mode = True) ou o Tracker para o FS_Node (mode = False). O campo 'id_mode' corresponde ao identificador do tipo
de pedido que o FS_Node pretende efetuar, sendo este 0 caso pretenda obter informações sobre quem possuí determinado ficheiro
e 1 caso pretenda repurtar uma alteração relativamente aos ficheiros que possuí para o FS_Tracker atualizar a sua base de dados.

Importante salientar que caso a mensagem contenha o campo 'mode' este não é contabilizado para o tamanho da mensagem.

Se a conexão for fechada a meio do envio de dados, a função fecha o socket do lado de quem está a tentar enviar os dados.
"""
def send_message_TCP(c, send_lock, message, mode, id_mode=None):
    try:

        # Quantidade de bytes da mensagem
        json_message = json.dumps(message).encode('utf-8')
        message_length = len(json_message)
        length_bytes = message_length.to_bytes(4, byteorder='big')

        # Verifica se a mensagem terá identificador
        if (mode):
            mode_bytes = id_mode.to_bytes(4, byteorder='big')

            # Enviar a mensagem
            send_lock.acquire()
            c.sendall(length_bytes + mode_bytes + json_message)
            send_lock.release()
        else:

            # Enviar a mensagem
            send_lock.acquire()
            c.sendall(length_bytes + json_message)
            send_lock.release()
    finally:
        c.close()


"""
Função que envia uma mensagem para outro FS_Node através de um socket UDP. Caso seja um pedido de um ficheiro, o argumento
"mode" terá associado o valor 0 e caso seja uma resposta a um pedido de ficheiro terá associado o valor 1. Importante salientar,
que o modo 0 envia em conjunto com os dados, informações adicionais de forma ao FS_Node que receber a mensagem conseguir devolver
o pacote pedido. No ínicio de cada mensagem é enviado ainda o tamanho da mesma, para o recetor ter a certeza da quantidade que tem
de ler.
"""
def send_message_UDP(mode, socket, data, destiny):
    if (mode==0):
        client_ip, client_port = socket.getsockname()
        message = [client_ip, client_port, data]
        json_message = json.dumps(message).encode('utf-8')
    elif (mode==1):
        json_message = json.dumps(data).encode('utf-8')
    message_length = len(json_message)
    length_bytes = message_length.to_bytes(4, byteorder='big')
    socket.sendto(length_bytes + json_message, destiny)
