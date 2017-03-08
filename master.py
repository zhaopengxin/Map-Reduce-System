import collections
import heapq
import os           
import shutil       
import socket       
from multiprocessing import Process
import threading
import time
import worker
import json
from queue import *
from helper import send_TCP_msg, set_up_TCP_socket, set_up_UDP_socket

class Master:

    def __init__(self, num_workers, port_number):
        #print()
        #print("master starts")
        self.num_workers = num_workers
        self.TCP_port = port_number
        self.UDP_port = port_number-1
        if os.path.exists('var'):
            shutil.rmtree('var')
        os.mkdir('var')
        self.reset()

        setup_thread = threading.Thread(target = self.run_setup_thread)
        setup_thread.start()

        s = set_up_TCP_socket(self.TCP_port)

        while True:

            clientsocket, address = s.accept()
            msg = ""

            if self.shutdown:
                break

            while True:
                msg_piece = clientsocket.recv(1024)
                msg = msg + msg_piece.decode("utf-8")
                if len(msg_piece) != 1024:
                    break
            clientsocket.close()

            self.handle_msg(msg)
            
        #print()
        #print("master ends")

    def reset(self):
        self.job_counter = 0
        self.worker_record = dict()
        self.working_workers = dict()
        self.ready_workers = []
        self.master_status = "ready"
        self.job_queue = Queue()
        self.current_job = None
        self.reduce_worker_num = 0
        self.distributed_files = []
        self.working_worker_files = dict()
        self.shutdown = False
        self.worker_reasign = False 

    def run_setup_thread(self):
        #print()
        #print("master: run_setup_thread starts")

        self.heartbeat_thread = threading.Thread(target = self.run_heartbeat_thread)
        self.heartbeat_thread.start()

        self.fault_thread = threading.Thread(target = self.run_fault_thread)
        self.fault_thread.start()

        for worker_id in range(self.num_workers):
            port_number = self.TCP_port + worker_id + 1
            master_port = self.TCP_port
            master_heartbeat_port = self.TCP_port-1
            p = Process(target = worker.Worker, args=(worker_id,port_number,master_port,master_heartbeat_port))
            p.start()
            self.worker_record[worker_id] = {
                "port_number": port_number,
                "process":p,
                "lastupdate":time.time()
            }    

        #print()
        #print("master: run_setup_thread ends")

    # a function used for handle both TCP message and UDP message
    def handle_msg(self, msg):
        #print()
        #print("master: handle_msg starts")
        msg_dict = json.loads(msg)
        if msg_dict['message_type'] == 'shutdown':
            for worker_id, worker in self.worker_record.items():
                worker['process'].terminate()
            self.shutdown = True
            return

        if msg_dict['message_type'] == 'status':
            # worker is ready to work
            if msg_dict['status'] == 'ready' or msg_dict['status'] == 'finished':
                worker_id = msg_dict['worker_number']
                if msg_dict['status'] == 'finished':
                    del self.working_workers[worker_id]
                    del self.working_worker_files[worker_id]
                self.ready_workers.append(worker_id)

            if msg_dict['status'] == 'ready' and self.worker_reasign:
                self.worker_reasign = False
                worker_id = msg_dict['worker_number']
                job_dict = self.current_job
                if self.master_status == 'map_stage':
                    del self.ready_workers[worker_id]
                    files_list = self.working_worker_files[worker_id]
                    worker_TCP_port = self.worker_record[worker_id]['port_number']
                    output_folder = os.path.join('./var/job-' + str(job_dict['job_id']) , 'mapper-output') 
                    msg_dict = {"message_type":"new_worker_job",
                                "input_files":files_list,
                                "executable":job_dict['mapper_executable'],
                                "output_directory":output_folder
                                }
                    send_TCP_msg(worker_TCP_port, msg_dict)
                if self.master_status == 'reduce_stage' and worker_id in self.working_workers:
                    del self.ready_workers[worker_id]
                    files_list = self.working_worker_files[worker_id]
                    worker_TCP_port = self.worker_record[worker_id]['port_number']
                    output_folder = os.path.join('./var/job-' + str(job_dict['job_id']) , 'reducer-output')
                    msg_dict = {"message_type":"new_worker_job",
                                "input_files":files_list,
                                "executable":job_dict['reducer_executable'],
                                "output_directory":output_folder
                                }
                    send_TCP_msg(worker_TCP_port, msg_dict)  

                #print()
                #print(str(msg_dict['worker_number']) + ' is ' + msg_dict['status'])
        # a new job is passed to master
        if msg_dict['message_type'] == 'new_master_job':
            print('get new job!')
            temp_file_folder = './var/job-' + str(self.job_counter)
            msg_dict['job_id'] = self.job_counter
            self.job_counter += 1
            os.mkdir(temp_file_folder)
            os.mkdir(os.path.join(temp_file_folder, 'mapper-output'))
            os.mkdir(os.path.join(temp_file_folder, 'grouper-output'))
            os.mkdir(os.path.join(temp_file_folder, 'reducer-output'))
            self.job_queue.put(msg_dict)

        if self.master_status == 'ready' and not self.job_queue.empty() and len(self.ready_workers) != 0:
            self.run_map_stage()
        elif self.master_status == 'map_stage' and len(self.working_workers) == 0 and len(self.ready_workers) == len(self.worker_record):
            self.run_group_stage()
            self.run_reduce_stage()
        elif self.master_status == 'reduce_stage' and len(self.working_workers) == 0:
            self.run_finished_stage()

            #self.reset()

        #print()
        #print("master: handle_msg ends")

    def run_map_stage(self):
        #print()
        print("master: run_map_stage starts")
        self.master_status = 'map_stage'
        self.current_job = self.job_queue.get()
        job_dict = self.current_job
        input_folder = job_dict['input_directory']
        input_files = os.listdir(input_folder)
        ready_worker_num = len(self.ready_workers)
        files_num = len(input_files)

        self.distributed_files = []
        # first-round distribution
        next_rd_worker = 0
        for i in range(ready_worker_num):
            if files_num == 0:
                break
            self.distributed_files.append([os.path.join(input_folder, input_files[i])])
            files_num -= 1
            next_rd_worker += 1
        # remain-round distribution
        pos = 0
        i = ready_worker_num
        while(files_num != 0):
            self.distributed_files[pos].append(os.path.join(input_folder, input_files[i]))
            i += 1
            files_num -= 1
            pos = (pos + 1) % ready_worker_num

        

        # for testing files distribution
        for files_list in self.distributed_files:
            for files in files_list:
                print(files)
            print()

        # send work information to workers
        pos = 0
        for files_list in self.distributed_files:
            worker_id = self.ready_workers[pos]
            self.working_workers[worker_id] = time.time()
            self.working_worker_files[worker_id] = files_list
            worker_TCP_port = self.worker_record[worker_id]['port_number']
            output_folder = os.path.join('./var/job-' + str(job_dict['job_id']), 'mapper-output')
            msg_dict = { "message_type":"new_worker_job",
                        "input_files":files_list,
                        "executable":job_dict['mapper_executable'],
                        "output_directory":output_folder
                        }
            send_TCP_msg(worker_TCP_port, msg_dict)
            pos += 1
        self.ready_workers = self.ready_workers[next_rd_worker:] 
        #print()
        #print("master: run_map_stage ends")

    def run_group_stage(self):
        #print()
        print("master: run_group_stage starts")
        self.master_status = 'group_stage'
        job_dict = self.current_job
        input_folder = os.path.join('./var/job-' + str(job_dict['job_id']) , 'mapper-output/')
        output_folder = os.path.join('./var/job-' + str(job_dict['job_id']) , 'grouper-output/')
        num_workers = len(self.ready_workers)
        self.reduce_worker_num = num_workers
        print("reduce_worker_num: " + str(self.reduce_worker_num))
        #TO CHECK
        #num_workers = len(self.worker_record)
        self.__staff_run_group_stage(input_folder, output_folder, num_workers)

        #print()
        #print("master: run_group_stage ends")

    def run_reduce_stage(self):
        #print()
        print("master: run_reduce_stage starts")
        self.master_status = 'reduce_stage'
        job_dict = self.current_job
        input_folder = os.path.join('./var/job-' + str(job_dict['job_id']) , 'grouper-output')
            
        input_files = os.listdir(input_folder)
        files_num = len(input_files)

        self.distributed_files = []
        # first-round distribution
        next_rd_worker = 0
        for i in range(self.reduce_worker_num):
            if files_num == 0:
                break
            self.distributed_files.append([os.path.join(input_folder, input_files[i])])
            files_num -= 1
            next_rd_worker += 1
        # remain-round distribution
        pos = 0
        i = self.reduce_worker_num
        while(files_num != 0):
            self.distributed_files[pos].append(os.path.join(input_folder, input_files[i]))
            i += 1
            files_num -= 1
            pos = (pos + 1) % reduce_worker_num

        # for testing files distribution
        for files_list in self.distributed_files:
            for files in files_list:
                print(files)
            print()

        pos = 0
        for files_list in self.distributed_files:
            worker_id = self.ready_workers[pos]
            self.working_workers[worker_id] = time.time()
            self.working_worker_files[worker_id] = files_list
            worker_TCP_port = self.worker_record[worker_id]['port_number']
            output_folder = os.path.join('./var/job-' + str(job_dict['job_id']), 'reducer-output')
            msg_dict = { "message_type":"new_worker_job",
                        "input_files":files_list,
                        "executable":job_dict['reducer_executable'],
                        "output_directory":output_folder
                        }
            send_TCP_msg(worker_TCP_port, msg_dict)
            pos += 1
        self.ready_workers = self.ready_workers[next_rd_worker:] 

        #print()
        print("working_workers: " + str(len(self.working_workers)))
        print("master: run_reduce_stage ends")

    def run_finished_stage(self):
        print()
        print("!!!JOB IS DONE!!!")
        #print()
        #print("master: run_finished_stage starts")
        self.master_status = 'ready'
        job_dict = self.current_job
        input_folder = os.path.join('./var/job-' + str(job_dict['job_id']), 'reducer-output')  
        output_folder = job_dict['output_directory']
        input_files = os.listdir(input_folder)
        if os.path.exists(output_folder):
            shutil.rmtree(output_folder)
        os.mkdir(output_folder)
        for input_file in input_files:
            src = os.path.join(input_folder, input_file)
            dst = os.path.join(output_folder, input_file)
            shutil.move(src, dst)

        if not self.job_queue.empty() and len(self.ready_workers) != 0:
            self.run_map_stage()
        #print()
        #print("master: run_finished_stage ends")

    def run_heartbeat_thread(self):
        #print()
        #print("master: run_heartbeat_thread starts")
        s = set_up_UDP_socket(self.UDP_port)

        while True:
            msg, address = s.recvfrom(1024)
            msg_dict = json.loads(msg.decode('utf-8'))
            worker_id = msg_dict['worker_number']
            self.worker_record[worker_id]['lastupdate'] = time.time()
            if worker_id in self.working_workers:
                self.working_workers[worker_id] = time.time()

            #print(str(worker_id) +  " is alive")
        #print()                   
        #print("master: run_heartbeat_thread ends")

    def run_fault_thread(self):
        #print()
        #print("master: run_fault_thread starts")
        while True:
            if self.shutdown:
                break
            threshold = time.time()-10
            pos = 0
            for worker_id, worker_info in self.worker_record.items():
                last_time = worker_info['lastupdate']
                if(last_time < threshold):
                    print('!!' + str(worker_id) + ' die')
                    self.worker_record[worker_id]['process'].terminate()
                    #del self.working_workers[worker_id]
                    port_number = self.TCP_port + worker_id + 1
                    master_port = self.TCP_port
                    master_heartbeat_port = self.TCP_port-1
                    p = Process(target = worker.Worker, args=(worker_id,port_number,master_port,master_heartbeat_port))
                    p.start()
                    self.worker_record[worker_id] = {
                        "port_number": port_number,
                        "process":p,
                        "lastupdate":time.time()
                    }

                    if (self.master_status == 'map_stage' or self.master_status == 'reduce_stage')and worker_id in self.working_workers:
                        self.worker_reasign = True

                pos += 1

            time.sleep(10)
        #print()
        #print("master: run_fault_thread ends")




    def __staff_run_group_stage(self, input_dir, output_dir, num_workers):
        # Loop through input directory and get all the files generated in Map stage
        filenames = []

        for in_filename in os.listdir(input_dir):
            filename = input_dir + in_filename

            # Open file, sort it now to ease the merging load later
            with open(filename, 'r') as f_in:
                content = sorted(f_in)

            # Write it back into the same file
            with open(filename, 'w+') as f_out:
                f_out.writelines(content)

            # Remember it in our list
            filenames.append(filename)

        # Create a new file to store ALL the sorted tuples in one single
        sorted_output_filename = os.path.join(output_dir, 'sorted.out')
        sorted_output_file = open(sorted_output_filename, 'w+')

        # Open all files in a single map command! Python is cool like that!
        files = map(open, filenames)

        # Loop through all merged files and write to our single file above
        for line in heapq.merge(*files):
            sorted_output_file.write(line)

        sorted_output_file.close()

        # Create a circular buffer to distribute file among number of workers
        grouper_filenames = []
        grouper_fhs = collections.deque(maxlen=num_workers)

        for i in range(num_workers):
            # Create temp file names
            basename = "file{0:0>4}.out".format(i)
            filename = os.path.join(output_dir, basename)

            # Open files for each worker so we can write to them in the next loop
            grouper_filenames.append(filename)
            fh = open(filename, 'w')
            grouper_fhs.append(fh)

        # Write lines to grouper output files, allocated by key
        prev_key = None
        sorted_output_file = open(os.path.join(output_dir, 'sorted.out'), 'r')

        for line in sorted_output_file:
            # Parse the line (must be two strings separated by a tab)
            tokens = line.rstrip().split("\t", 2)
            assert len(tokens) == 2, "Error: improperly formatted line"
            key, value = tokens

            # If it's a new key, then rotate circular queue of grouper files
            if prev_key != None and key != prev_key:
                grouper_fhs.rotate(1)

            # Write to grouper file
            fh = grouper_fhs[0]
            fh.write(line)

            # Update most recently seen key
            prev_key = key

        # Close grouper output file handles
        for fh in grouper_fhs:
            fh.close()

        # Delete the sorted output file
        sorted_output_file.close()
        os.remove(sorted_output_filename)

        # Return array of file names generated by grouper stage
        return grouper_filenames
