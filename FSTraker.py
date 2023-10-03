import socket
import threading
import pickle

class FSTracker:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.clients = {}  # Dicionário para armazenar informações dos FS_Node conectados

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)

        print(f"FS_Tracker ativo em {self.host}:{self.port}.")

        while True:
            client_socket, client_address = server_socket.accept()
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    def handle_client(self, client_socket):
        try:
            data = client_socket.recv(1024)
            if data:
                message = pickle.loads(data)
                if message['type'] == 'register':
                    self.register_node(client_socket, message['node_info'])
                elif message['type'] == 'update':
                    self.update_node(message['node_info'])
                elif message['type'] == 'locate':
                    self.locate_file(client_socket, message['filename'])
            client_socket.close()
        except Exception as e:
            print(f"Erro ao lidar com cliente: {str(e)}")

    def register_node(self, client_socket, node_info):
        node_address = client_socket.getpeername()
        self.clients[node_address] = node_info
        print(f"Node {node_info['name']} registado com sucesso.")

    def update_node(self, node_info):
        node_address = (node_info['ip'], node_info['port'])
        if node_address in self.clients:
            self.clients[node_address]['files'] = node_info['files']
            print(f"Node {self.clients[node_address]['name']} atualizado.")

    def locate_file(self, client_socket, filename):
        locations = []
        for address, info in self.clients.items():
            if filename in info['files']:
                locations.append({'name': info['name'], 'address': address})
        response = {'type': 'location_response', 'locations': locations}
        client_socket.send(pickle.dumps(response))

if __name__ == "__main__":
    tracker = FSTracker('10.4.4.1', 9090)
    tracker.start()
