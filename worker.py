import socket       
from multiprocessing import Process
import threading
import time
import json
from helper import send_TCP_msg, set_up_TCP_socket
import sh
import os

class Worker:
    def __init__(self, worker_id, port_number, master_port, master_heartbeat_port):
        #print()
        #print("worker " + str(worker_id) + ": starts")
        self.worker_id = worker_id
        self.TCP_port = port_number
        self.master_TCP_port = master_port
        self.master_UDP_port = master_heartbeat_port

        setup_thread = threading.Thread(target = self.run_setup_thread)
        setup_thread.start()

        s = set_up_TCP_socket(self.TCP_port)

        while True:
            clientsocket, address = s.accept()
            msg = ""
            while True:
                msg_piece = clientsocket.recv(1024)
                msg = msg + msg_piece.decode("utf-8")
                if len(msg_piece) != 1024:
                    break
            clientsocket.close()
            self.handle_msg(msg)
        #print()
        #print("worker " + str(worker_id) + " ends")

    def run_setup_thread(self):
        #print()
        #print("worker " + str(self.worker_id) + ": run_setup_thread starts")
        heartbeat_thread = threading.Thread(target = self.run_heartbeat_thread)
        heartbeat_thread.start()
        
        msg_dict = {"message_type":"status",
                    "worker_number":self.worker_id,
                    "status":"ready"} 
        send_TCP_msg(self.master_TCP_port, msg_dict)

        #print()
        #print("worker " + str(self.worker_id) + ": run_setup_thread ends")  

    def handle_msg(self, msg):
        #print(str(self.worker_id) + " starts")
        #print()
        #print("worker " + str(self.worker_id) + ": handle_msg starts")
        job_dict = json.loads(msg)
        if job_dict['message_type'] == 'new_worker_job':
            output_folder = job_dict['output_directory']
            files_list = job_dict['input_files']
            executable = job_dict['executable']
            
            for input_file in files_list:
                run = sh.Command(executable)
                data = open(input_file)
                output_file = os.path.join(output_folder, input_file.split('/')[-1])
                #print(input_file)
                #print(output_file)
                run(_in = data, _out = output_file)
                data.close()

            msg_dict = {"message_type":"status",
                        "worker_number":self.worker_id,
                        "status":"finished"
                        }
        #print(str(self.worker_id) + " ends")
        send_TCP_msg(self.master_TCP_port, msg_dict)
        #print()
        #print("worker " + str(self.worker_id) + ": handle_msg ends")



    def run_heartbeat_thread(self):
        #print()
        #print("worker " + str(self.worker_id) + ": run_heartbeat_thread starts")
        self.UDP_PORT = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        msg_dict = {"message_type":"heartbeat",
                    "worker_number":self.worker_id}
        addr = ("127.0.0.1", self.master_UDP_port)
        msg = json.dumps(msg_dict).encode('utf-8')
        while True:
            self.UDP_PORT.sendto(msg, addr)
            time.sleep(2)
        #print()
        #print("worker " + str(self.worker_id) + ": run_heartbeat_thread ends")
