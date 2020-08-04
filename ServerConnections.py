import threading
import socket
import pickle
import server_info as si


class ConnectionClass():
    # Used to help track clients rather than just by their connection
    # also is used as the median for communication between Server()
    # and ServerConnection() with regards to a specific client
    def __init__(self, conn, addr):
        self.conn = conn
        self.addr = addr

        self.command = None
        self.data = None

    def send(self, msg):
        self.conn.send(msg)

    def recv(self, header):
        return self.conn.recv(header)

    def recv_len(self, header, info_format):
        return self.conn.recv(header).decode(info_format)

    def close(self):
        self.conn.close()

    def get_addr(self):
        return self.addr



class ServerConnection():
    # ServerConnection is in charge of handleing all incoming and outgoing
    # data transfers with all clients
    # There is currently print statements here but this is for tracking when
    # debugging. Comment them out or delete them when running to reduce speed
    def __init__(self):
        self.header = si.header
        self.server_ip = si.server_ip
        self.addr = (self.server_ip, si.port)
        self.info_format = si.info_format

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(self.addr)

        self.running = True

        self.client_connections = []

    #Communication Functions
    def send(self, obj, conn_class):
        message = pickle.dumps(obj)
        msg_length = len(message)
        send_length = str(msg_length).encode(self.info_format)
        send_length += b' ' * (self.header - len(send_length))
        conn_class.send(send_length)
        conn_class.send(message)

    def handle_inc_com(self, conn_class):
        try:
            obj_length = conn_class.recv_len(self.header, self.info_format)

            if obj_length:
                obj_length = int(obj_length)
                pickled_obj = conn_class.recv(obj_length)
                command = pickle.loads(pickled_obj)
                if command == '!QUIT':
                    return False
                conn_class.command = command
                return True
        except:
            return False

    def handle_out_data(self, conn_class):
        while True:
            if not conn_class.data is None:
                try:
                    self.send(conn_class.data, conn_class)
                    conn_class.data = None
                    return True
                except:
                    return False

    def handle_client(self, conn_class):
        print(f"[NEW CONNECTION] {conn_class.addr} connected.")
        connected = True

        while connected:
            connected = self.handle_inc_com(conn_class)
            if connected:
                connected = self.handle_out_data(conn_class)

        print(f"[CLIENT DISCONNECTED] {conn_class.get_addr()} has disconnected.")
        self.client_connections.remove(conn_class)
        conn_class.close()
    


    #Main Server Loop
    def server_main(self):
        self.server_socket.listen()

        while True:
            # We create a thread for each new client
            conn, addr = self.server_socket.accept()
            new_client = ConnectionClass(conn, addr)
            self.client_connections.append(new_client)
            new_client_thread = threading.Thread(target=self.handle_client, args=([new_client]))
            new_client_thread.start()

            # We do threading.activecount - 2 to track current active clients
            # We subtract 2 as this function itself is called as a thread in Server.py
            print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 2}")

            