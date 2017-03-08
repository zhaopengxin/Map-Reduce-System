import click, json
from socket import *

def set_up_TCP_socket(port):
    # create an INET, UDP socket
    s = socket(AF_INET, SOCK_STREAM)
    # connect to the server
    s.bind(('127.0.0.1', port))
    # TO CHECK 
    # queue size is set to be 10 now
    s.listen(10)
    return s

def set_up_UDP_socket(port):
    # create an INET, UDP socket
    s = socket(AF_INET, SOCK_DGRAM)
    # connect to the server
    s.bind(('127.0.0.1', port))
    return s

def send_TCP_msg(target_port, msg_dict):
    p = socket(AF_INET, SOCK_STREAM)
    p.connect(('127.0.0.1', target_port))
    msg = json.dumps(msg_dict).encode('utf-8')
    p.sendall(msg)
    p.close() 


# Configure command line options
DEFAULT_PORT_NUM = 6000
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

@click.command(context_settings=CONTEXT_SETTINGS)

@click.option("--port_number", "-p", "port_number",
    default=DEFAULT_PORT_NUM,
    help="The port the master is listening on, default " + str(DEFAULT_PORT_NUM))

def main(port_number=DEFAULT_PORT_NUM):
    # Temp job to send to master (values can be changed)
    job_dict = {
        "message_type": "new_master_job",
        "input_directory": "./input/sample1",
        "output_directory": "./output",
        "mapper_executable": "./exec/word_count/map.py",
        "reducer_executable": "./exec/word_count/reduce.py"
    }

    message = json.dumps(job_dict)

    # Send the data to the port that master is on
    sock = socket(AF_INET, SOCK_STREAM)

    sock.connect(("127.0.0.1", port_number))
    sock.sendall(str.encode(message))
    sock.close()


if __name__ == "__main__":
  main()
