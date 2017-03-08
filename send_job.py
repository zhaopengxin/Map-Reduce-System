import click, json
from socket import *

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
        "input_directory": "./input",
        "output_directory": "./output",
        "mapper_executable": "./map",
        "reducer_executable": "./reduce"
    }

    message = json.dumps(job_dict)

    # Send the data to the port that master is on
    try:
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(("localhost", port_number))
        sock.sendall(str.encode(message))
        sock.close()
    except error:
        print("Failed to send job to master.")

if __name__ == "__main__":
  main()
