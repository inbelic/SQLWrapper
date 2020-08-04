import threading
import socket
import pickle
import pandas as pd
import server_info as si


class ClientConnection():
    def __init__(self):
        self.header = si.header
        self.server_ip = si.server_ip
        self.addr = (self.server_ip, si.port)
        self.info_format = si.info_format

        self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.conn.connect(self.addr)
        
        self.running = True


    #Communication Functions
    def check_inc(self):
        while self.running:
            try:
                obj_length = self.conn.recv(self.header)
                if obj_length:
                    obj_length = int(obj_length)
                    pickled_obj = self.conn.recv(obj_length)
                    new_data = pickle.loads(pickled_obj)
                    if type(new_data) == str:
                        print(new_data)
                    elif type(new_data) == dict:
                        print(pd.DataFrame.from_dict(new_data))
            except:
                print("[SERVER SHUTDOWN]")
                self.running = False


    def send(self, obj):
        message = pickle.dumps(obj)
        msg_length = len(message)
        send_length = str(msg_length).encode(self.info_format)
        send_length += b' ' * (self.header - len(send_length))
        self.conn.send(send_length)
        self.conn.send(message)


    def check_input(self):
        while self.running:
            try:
                command = str(input())
                self.send(command)
            except:
                print("[UNABLE TO COMMUNICATE WITH SERVER]")
                self.running = False


    #Main Client Loop
    def client_main(self):
        check_inc_thread = threading.Thread(target=self.check_inc)
        check_inc_thread.start()
        input_thread = threading.Thread(target=self.check_input)
        input_thread.start()

        print(f"[CONNECTED] Connected to {self.server_ip}")

        while self.running:
            pass
        





local_client = ClientConnection()
local_client.client_main()