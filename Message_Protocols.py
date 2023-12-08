"""
Ficheiro que contem algumas das funções relativas à troca de mensagens entre o FS_Tracker e os FS_Nodes e entre os FS_Nodes.
"""

import json
import socket
import hashlib
import zlib



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
    size_packet_bin = c.recv(4)
    if not size_packet_bin:
        return -1
    size_packet = int.from_bytes(size_packet_bin, byteorder='big')

    message = None
    if (mode):
        id_mode_bytes = c.recv(4)
        if not id_mode_bytes:
            return -1
        id_mode = int.from_bytes(id_mode_bytes, byteorder='big')

        if id_mode==0:
            # formato packet -> size_packet + mode + filename

            # Recebe o nome do ficheiro
            filename_bin = b''
            size_filename = size_packet - 4
            while len(filename_bin) < size_filename:
                data = c.recv(size_filename - len(filename_bin))
                if not data:
                    return -1
                filename_bin += data
            
            filename_chunks = [filename_bin[i:i+8] for i in range(0, len(filename_bin), 8)]
            filename = ''.join(chr(int(chunk, 2)) for chunk in filename_chunks)

            message = filename

        if id_mode==1:
            # formato packet -> size_packet + mode + filename_size + filename + n_packets + packets_Owned + filename_size + filename + n_packets + packets_Owned + ...

            message = []
            while size_packet-4 > 0: # -4 por causa do campo mode do cabeçalho

                # Recebe o número de bytes do nome do ficheiro
                filename_size_bin = c.recv(4)
                if not filename_size_bin:
                    return -1
                filename_size = int.from_bytes(filename_size_bin, byteorder='big')
            
                # Recebe o nome do ficheiro
                filename_bin = b''
                while len(filename_bin) < filename_size:
                    data = c.recv(filename_size - len(filename_bin))
                    if not data:
                        return -1
                    filename_bin += data
                
                filename_chunks = [filename_bin[i:i+8] for i in range(0, len(filename_bin), 8)]
                filename = ''.join(chr(int(chunk, 2)) for chunk in filename_chunks)

                # Recebe o número de pacotes que compõem o ficheiro
                n_packets_bin = c.recv(4)
                if not n_packets_bin:
                    return -1
                n_packets = int.from_bytes(n_packets_bin, byteorder='big')

                # Recebe o byte que identifica se o FS_Node tem o ficheiro completo ou não
                byte_id_bin = c.recv(1)
                byte_id = int.from_bytes(byte_id_bin, byteorder='big')

                if byte_id==1:
                    message.append([filename, n_packets, -1])
                    size_packet = size_packet - filename_size - 9
                elif byte_id==0:

                    # Recebe o número que identifica quantos bytes ocupa o inteiro que vem a seguir (inteiro que representa os pacotes que o FS_Node possuí do ficheiro)
                    num_bytes_packets_owned_bin = c.recv(4)
                    num_bytes_packets_owned = int.from_bytes(num_bytes_packets_owned_bin, byteorder='big')

                    # Recebe o inteiro que representa os pacotes que o FS_Node possuí do ficheiro
                    packets_owned_bin = c.recv(num_bytes_packets_owned)
                    packets_owned = int.from_bytes(packets_owned_bin, byteorder='big')

                    message.append([filename, n_packets, packets_owned])
                    size_packet = size_packet - filename_size - num_bytes_packets_owned - 13

        if id_mode==2 or id_mode==3:
            # formato packet -> size_packet + mode + filename + packet_index

            # Recebe o nome do ficheiro
            filename_bin = b''
            size_filename = size_packet - 8
            while len(filename_bin) < size_filename:
                data = c.recv(size_filename - len(filename_bin))
                if not data:
                    return -1
                filename_bin += data
            
            filename_chunks = [filename_bin[i:i+8] for i in range(0, len(filename_bin), 8)]
            filename = ''.join(chr(int(chunk, 2)) for chunk in filename_chunks)

            # Recebe o indíce do pacote (mode==2) ou os pacotes que o FS_Node tem (mode==3)
            packet_index_bin = c.recv(4)
            if not packet_index_bin:
                return -1
            packet_index = int.from_bytes(packet_index_bin, byteorder='big')

            message = [filename, packet_index]
    
    else:

        if (size_packet>0):
            # formato packet -> size_packet + size_file + byte_identificador + info + byte_identificador + info + ...

            # Recebe o número de pacotes que compõem o ficheiro
            n_packets_bin = c.recv(4)
            if not n_packets_bin:
                return -1
            n_packets = int.from_bytes(n_packets_bin, byteorder='big')

            size_packet -= 4 # n_packets = 4
            message = [n_packets]
            while size_packet > 0:

                # Recebe o byte que identifica se o que vem a seguir é do tipo (193.0.1.2, 9090) ou [(172.0.1, 9090), 61253]
                byte_id_bin = c.recv(1)
                byte_id = int.from_bytes(byte_id_bin, byteorder='big')

                # Recebe o número de bytes do endereço IP
                ip_size_bin = c.recv(4)
                if not ip_size_bin:
                    return -1
                ip_size = int.from_bytes(ip_size_bin, byteorder='big')
            
                # Recebe o nome do ficheiro
                ip_bin = b''
                while len(ip_bin) < ip_size:
                    data = c.recv(ip_size - len(ip_bin))
                    if not data:
                        return -1
                    ip_bin += data
                
                ip_chunks = [ip_bin[i:i+8] for i in range(0, len(ip_bin), 8)]
                ip = ''.join(chr(int(chunk, 2)) for chunk in ip_chunks)
            
                # Recebe a porta
                port_bin = c.recv(4)
                if not port_bin:
                    return -1
                port = int.from_bytes(port_bin, byteorder='big')

                if byte_id==0:
                    message.append((ip, port))
                    size_packet = size_packet - ip_size - 5
                elif byte_id==1:
                    
                    # Recebe o número que identifica quantos bytes ocupa o inteiro que vem a seguir (inteiro que representa os pacotes que o FS_Node possuí do ficheiro)
                    num_bytes_packets_owned_bin = c.recv(4)
                    num_bytes_packets_owned = int.from_bytes(num_bytes_packets_owned_bin, byteorder='big')

                    # Recebe o inteiro que representa os pacotes que o FS_Node possuí do ficheiro
                    packets_owned_bin = c.recv(num_bytes_packets_owned)
                    packets_owned = int.from_bytes(packets_owned_bin, byteorder='big')

                    message.append([(ip, port), packets_owned])
                    size_packet = size_packet - ip_size - num_bytes_packets_owned - 9

    if (mode):
        message = (id_mode, message)

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

        # Verifica se a mensagem terá identificador
        packet = None
        if (mode):
            mode_bin = id_mode.to_bytes(4, byteorder='big')
            # c.sendall(length_bytes + mode_bytes + json_message)

            if id_mode==0:
                # Usado quando o FS_Node quer perguntar ao FS_Tracker quem tem determinado ficheiro
                # argumento message -> filename
                # formato packet -> size_packet + mode + filename

                size_packet = 4 # size_mode = 4
                filename_bin = b''.join(format(ord(char), '08b').encode('utf-8') for char in message)
                size_packet += len(filename_bin)
                size_packet_bin = size_packet.to_bytes(4, byteorder='big')
                packet = size_packet_bin + mode_bin + filename_bin

            if id_mode==1:
                # Usado para informar o FS_Tracker dos ficheiros iniciais que o FS_Node tem
                # argumento message -> ([fileName, n_packets, packets_Owned], [fileName, n_packets, packets_owned], ...)
                # formato packet -> size_packet + mode + filename_size + filename + n_packets + packets_Owned + filename_size + filename + n_packets + packets_Owned + ...
                # packets_Owned: Este campo é composto por campos diferentes dependendo se o ficheiro está completo ou incompleto
                # ficheiro completo: Apenas um campo, file_completed_byte que ocupa 1 byte
                # ficheiro incompleto: 3 campos, file_completed_byte + packets_Owned_size + packets_Owned


                byte_id_notCompleted = 0
                byte_id_Completed = 1
                byte_id_notCompleted_bin = byte_id_notCompleted.to_bytes(1, byteorder='big')
                byte_id_Completed_bin = byte_id_Completed.to_bytes(1, byteorder='big')

                size_packet = 4 # size_mode = 4
                packet = b''
                for fileName, n_packets, packets_owned in message:
                    filename_bin = b''.join(format(ord(char), '08b').encode('utf-8') for char in fileName)
                    filename_size = len(filename_bin)
                    filename_size_bin = filename_size.to_bytes(4, byteorder='big')
                    n_packets_bin = n_packets.to_bytes(4, byteorder='big')


                    # Insere um identificador a 1 a informar que o ficheiro está completo
                    if packets_owned==-1:
                        size_packet += filename_size + 9 # filename_size + n_packets + byte_id_Completed = 9
                        packet += filename_size_bin + filename_bin + n_packets_bin + byte_id_Completed_bin
                    
                    # Insere um identificador a 0 a informar que o ficheiro está incompleto
                    else:
                        # Calcula quantos bits o inteiro que representa os pacotes que o FS_Node possuí ocupa
                        num_bits_packets_owned = packets_owned.bit_length()
                        # Converte para o número de bytes necessários
                        num_bytes_packets_owned = (num_bits_packets_owned + 7) // 8
                        num_bytes_packets_owned_bin = num_bytes_packets_owned.to_bytes(4, byteorder='big')
                        packets_owned_bin = packets_owned.to_bytes(num_bytes_packets_owned, byteorder='big')

                        size_packet += filename_size + num_bytes_packets_owned + 13 # filename_size + n_packets + byte_id_Completed + num_bytes_packets_owned = 13
                        packet += filename_size_bin + filename_bin + n_packets_bin + byte_id_notCompleted_bin + num_bytes_packets_owned_bin + packets_owned_bin

                size_packet_bin = size_packet.to_bytes(4, byteorder='big')
                packet = size_packet_bin + mode_bin + packet
            
            elif id_mode==2 or id_mode==3:
                # Usado para informar o FS_Tracker de alterações relativas a um ficheiro
                # argumento message -> (fileName, packet_index)
                # formato packet -> size_packet + mode + filename + packet_index

                filename_bin = filename_bin = b''.join(format(ord(char), '08b').encode('utf-8') for char in message[0])
                packet_index_bin = message[1].to_bytes(4, byteorder='big')
                size_packet = len(filename_bin) + 8 # mode + packet_index = 8
                size_packet_bin = size_packet.to_bytes(4, byteorder='big')
                packet = size_packet_bin + mode_bin + filename_bin + packet_index_bin
            
        else:
            # Usado pelo FS_Tracker para informar o FS_Node quais os FS_Nodes que têm o ficheiro que ele pediu
            # argumento message -> [size_file, (172.0.1, 9090), (193.0.2, 10001), [(172.0.4, 20010), 61253], (193.0.1.3, 9090), [(172.0.1.3, 9090), 5723], ...]
            # formato packet -> size_packet + size_file + byte_identificador + info + byte_identificador + info + ...
            # byte_identificador: determina se o campo seguinte é do tipo (193.0.1.2, 9090) ou [(172.0.1, 9090), 61253]
            # info: está associado ao byte_identificador e pode ser (193.0.1.2, 10015) ou [(172.0.1, 20017), 61253]
            # Se for do tipo [(172.0.1, 20017), 61253] o info incluí ainda um campo packets_Owned_size e packets_Owned

            byte_id_notList = 0
            byte_id_list = 1
            byte_id_notList_bin = byte_id_notList.to_bytes(1, byteorder='big')
            byte_id_list_bin = byte_id_list.to_bytes(1, byteorder='big')

            if len(message):
                packet = b''
                size_packet = 4 # n_packets = 4
                n_packets_bin = message[0].to_bytes(4, byteorder='big')
                for info in message[1:]:
                    if not isinstance(info, list):
                        ip, port = info

                        # Insere um identificador a 0 a informar que não é uma lista
                        ip_bin = b''.join(format(ord(char), '08b').encode('utf-8') for char in ip)
                        ip_size = len(ip_bin)
                        ip_size_bin = ip_size.to_bytes(4, byteorder='big')
                        port_bin = port.to_bytes(4, byteorder='big')
                        size_packet += ip_size + 5 # identificador + port = 5
                        packet += byte_id_notList_bin + ip_size_bin + ip_bin + port_bin
                    else:
                        addr, packets_owned = info
                        ip, port = addr

                        # Insere um identificador a 1 a informar que é uma lista de dois elementos
                        ip_bin = b''.join(format(ord(char), '08b').encode('utf-8') for char in ip)
                        ip_size = len(ip_bin)
                        ip_size_bin = ip_size.to_bytes(4, byteorder='big')
                        port_bin = port.to_bytes(4, byteorder='big')

                        # Calcula quantos bits o inteiro que representa os pacotes que o FS_Node possuí ocupa
                        num_bits_packets_owned = packets_owned.bit_length()
                        # Converte para o número de bytes necessários
                        num_bytes_packets_owned = (num_bits_packets_owned + 7) // 8
                        num_bytes_packets_owned_bin = num_bytes_packets_owned.to_bytes(4, byteorder='big')
                        packets_owned_bin = packets_owned.to_bytes(num_bytes_packets_owned, byteorder='big')

                        size_packet += ip_size + num_bytes_packets_owned + 9 # identificador + port + num_bytes_packets_owned = 9
                        packet += byte_id_list_bin + ip_size_bin + ip_bin + port_bin + num_bytes_packets_owned_bin + packets_owned_bin
            
                size_packet_bin = size_packet.to_bytes(4, byteorder='big')
                packet = size_packet_bin + n_packets_bin + packet
            else:
                size_packet = 0
                size_packet_bin = size_packet.to_bytes(4, byteorder='big')
                packet = size_packet_bin

        # Enviar a mensagem
        send_lock.acquire()
        c.sendall(packet)
        send_lock.release()
    except socket.error:
        c.close()


"""
Função que envia uma mensagem para outro FS_Node através de um socket UDP. Caso seja um pedido de um ficheiro, o argumento
"mode" terá associado o valor 0 e caso seja uma resposta a um pedido de ficheiro terá associado o valor 1. Importante salientar,
que o modo 0 envia em conjunto com os dados, informações adicionais de forma ao FS_Node que receber a mensagem conseguir devolver
o pacote pedido. No ínicio de cada mensagem é enviado ainda o tamanho da mesma, para o recetor ter a certeza da quantidade que tem
de ler.
"""
def send_message_UDP(mode, socket, filename, packet_index, packet, destiny):
    if (mode==0):
        # Usado para pedir pacotes de ficheiros a outros FS_Nodes
        # formato packet -> mode + checksum + filename_size + filename + packet_index

        mode_bin = mode.to_bytes(4, byteorder='big')
        filename_bin = b''.join(format(ord(char), '08b').encode('utf-8') for char in filename)
        filename_size = len(filename_bin)
        filename_size_bin = filename_size.to_bytes(4, byteorder='big')
        packet_index_bin = packet_index.to_bytes(4, byteorder='big')

        # Calcular o checksum dos campos do pacote
        checksum = zlib.crc32(mode_bin+filename_size_bin+filename_bin+packet_index_bin) & 0xFFFFFFFF
        checksum_bin = checksum.to_bytes(4, 'big')

        packet = mode_bin + checksum_bin + filename_size_bin + filename_bin + packet_index_bin
    elif (mode==1):
        # Usado para enviar pacotes de ficheiros a outros FS_Nodes
        # formato packet -> mode + hash + filename_size + filename + packet

        mode_bin = mode.to_bytes(4, byteorder='big')
        filename = filename + str(packet_index)
        filename_bin = b''.join(format(ord(char), '08b').encode('utf-8') for char in filename)
        filename_size = len(filename_bin)
        filename_size_bin = filename_size.to_bytes(4, byteorder='big')
        
        # Calcular o valor de hash do pacote
        hasher = hashlib.sha256()
        hasher.update(mode_bin+filename_size_bin+filename_bin+packet)
        hash = hasher.digest()

        packet = mode_bin + hash + filename_size_bin + filename_bin + packet
    
    socket.sendto(packet, destiny)


"""

"""
def receive_message_UDP(socket):
    message, sender_address = socket.recvfrom(2000)

    # Lê o modo
    modo_bin = message[:4]
    modo = int.from_bytes(modo_bin, byteorder='big')

    packet = None
    if (modo==0):
        # Lê a hash recebida
        checksum_bin = message[4:8]

        # Calcula o checksum da mensagem recebida sem contar com o campo checksum
        checksum_packet = zlib.crc32(message[:4]+message[8:]) & 0xFFFFFFFF
        checksum_packet_bin = checksum_packet.to_bytes(4, 'big')

        if checksum_bin==checksum_packet_bin:
            # Lê o tamanho do nome do ficheiro
            filename_size_bin = message[8:12]
            filename_size = int.from_bytes(filename_size_bin, byteorder='big')

            # Lê o nome do ficheiro
            i = 12+filename_size
            filename_bin = message[12:i]
            filename_chunks = [filename_bin[i:i+8] for i in range(0, len(filename_bin), 8)]
            filename = ''.join(chr(int(chunk, 2)) for chunk in filename_chunks)

            # Lê o índice do pacote
            packet_index_bin = message[i:]
            packet_index = int.from_bytes(packet_index_bin, byteorder='big')

            packet = [modo, filename, packet_index]
        else:
            return -1
    elif (modo==1):
        # Lê a hash recebida
        hash_bin = message[4:36]

        # Calcula a hash da mensagem recebida sem contar com o campo hash
        hasher = hashlib.sha256()
        hasher.update(message[:4]+message[36:])
        hash_packet = hasher.digest()

        if hash_bin==hash_packet:
            # Lê o tamanho do nome do ficheiro
            filename_size_bin = message[36:40]
            filename_size = int.from_bytes(filename_size_bin, byteorder='big')

            # Lê o nome do ficheiro
            i = 40+filename_size
            filename_bin = message[40:i]
            filename_chunks = [filename_bin[i:i+8] for i in range(0, len(filename_bin), 8)]
            filename = ''.join(chr(int(chunk, 2)) for chunk in filename_chunks)

            # Lê o pacote
            packet_data = message[i:]

            packet = [modo, filename, packet_data]
        else:
            return -1

    packet.insert(1, sender_address)
    
    return packet
