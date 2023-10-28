"""
Função responsável por receber e traduzir mensagens, sendo também responsável por verificar se o FS_Node ou FS_Tracker
não fecharam a conexão quer durante a transmissão de uma mensagem ou quando não estão a transmitir. Importante referir
que o argumento 'mode' refere-se ao tipo de mensagem que esperamos receber. Este é igual a 0 nos casos em que não estamos à espera de
receber um pedido, por exemplo, quando o servidor estabelece conexão com um FS_Node e este lhe manda os ficheiros que possuí, ou então
quando o FS_Node recebe do FS_Tracker a resposta de um pedido de "get fileX". Desta forma, a função pode ser usada pelo FS_Tracker e FS_Node,
tornando-se genérica.

O loop assegura que lemos a mensagem toda do socket buffer, caso esta ainda não esteja toda no buffer e a thread já esteja a tentar ler.

Caso a máquina que está a enviar os dados feche a conexão em qualquer momento, a função devolve -1, caso contrário, se o argumento 'mode' estiver a
0 devolve os dados sem especificar o tipo de pedido, já se estiver a 1, devolve um tuplo de dois elementos, sendo o primeiro elemento o inteiro identificador
do pedido e o segundo campo os dados.

Importante salientar que caso a mensagem contenha o campo 'mode' este não é contabilizado para o tamanho da mensagem.
"""
def recieve_message(c, mode):
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

        message = eval(message.decode('utf-8'))
    
    if (mode==1):
        message = (request, message)

    return message

"""
Função responsável por coverter os tipos de dados e estruturas para binário de forma a poder mandá-los para o respetivo destinatário, podendo
este ser um FS_Tracker ou FS_Node. O campo 'mode' especifica se o mensageiro pretende adicionar o campo do modo à mensagem ou não, sendo
este igual a False, caso não queira, e igual a True, caso queira. Por sua vez, o campo 'id_mode' corresponde ao id do tipo de pedido que o mesageiro
pretende efetuar, tendo de colocar, portanto, o campo 'mode' igual a 1.

Importante salientar que caso a mensagem contenha o campo 'mode' este não é contabilizado para o tamanho da mensagem.
"""
def send_message(c, message, mode, id_mode=None):
    message_length = len(message)
    length_bytes = message_length.to_bytes(4, byteorder='big')
    if (mode):
        id_mode_bytes = id_mode.to_bytes(4, byteorder='big')
        c.sendall(length_bytes + id_mode_bytes + message.encode('utf-8'))
    else:
        c.sendall(length_bytes + message.encode('utf-8'))
