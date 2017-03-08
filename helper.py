import json
from socket import *

def set_up_TCP_socket(port):
    s = socket(AF_INET, SOCK_STREAM)
    s.bind(('127.0.0.1', port))
    s.listen(20)
    return s

def set_up_UDP_socket(port):
    s = socket(AF_INET, SOCK_DGRAM)
    s.bind(('127.0.0.1', port))
    return s

def send_TCP_msg(target_port, msg_dict):
    p = socket(AF_INET, SOCK_STREAM)
    p.connect(('127.0.0.1', target_port))
    msg = json.dumps(msg_dict).encode('utf-8')
    p.sendall(msg)
    p.close() 


