#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Universidade de Lisboa
Faculdade de Ciencias
Departamento de Informatica
Licenciatura em Tecnologias da Informacao 2017/2018

Sistemas Operativos

Projecto

"""
__author__ = "Pedro Gritter, 44964, Pedro Romao, 46579, Francisco Horta Correia, 45098"
__email__ = "fc44964@alunos.fc.ul.pt, fc46579@alunso.fc.ul.pt, fc45098@alunos.fc.ul.pt"
__copyright__ = "Sistemas Operativos, LTI, DI/FC/UL, 2017"
__maintainer__ = "Pedro Gritter"
__version__ = "3.1.0"

import os, argparse, zipfile, time, signal, struct
from multiprocessing import Process, Queue, Semaphore, Value, Lock


# Pzip is a tool to compress or decompress files


# Helper functions
def filelist():
    """
    Gets file names from stdin.
    :return: a list of strings, each string is a file name given by the user.
    """
    file_list = []
    fileflag = True                                         # Iniciates the primary flag as True
    while fileflag:

        try:                                                # If the user as not given the file names, this fuction asks the user to insert the names of the files he wants to compress/decompress
            print("Insert name of the file you want to compresse/decompress. To continue enter 'end'")
            new_file = str(input("File: "))

            if new_file != "end" and os.path.isfile("./" + new_file) is False:
                print("File not found. Do you want to exit(yes/no)?")
                answer = str(input("Answer: "))

                if answer == "yes" or answer == "Yes":
                    fileflag = False

                else:
                    continue

            if new_file == "end" or new_file == "":
                fileflag = False

            file_list.append(new_file)

        except KeyboardInterrupt:                           # User interrupt sets the primary flag to False and ends the cycle
            fileflag = False

    return file_list


def doneqtolist(start, end, nprocesses, totalsize):
    """
    Converts the shared queue(done_q) into a usable list by
    the history function.
    :param start: must be float, represents starting time of program.
    :param end: must be float, represents ending time of program.
    :param nprocesses: must be int, represents number of processes.
    :param totalsize: must be int, represents total number of bytes processed by
    the program.
    :return: a list in the format needed to write a history file.
    """
    # Stores operations time in a callable list
    dq = [start, end, nprocesses, totalsize]
    while not done_q.empty():
        item = done_q.get()
        if item is None:
            pass
        else:
            dq.append(item)
    return dq


def operation_info():
    """
    Helper function that displays the shared info among processes
    and total time.
    """
    end = time.time()
    totaltime = end - pzip_start
    print("Files Processed: " + str(filesDone.value) + " files.")
    print("Total output size: " + str((total_output_size.value)/1024) + "Kb")
    print("Total time processing: " + str(totaltime))


# Signals & Alarms#
def signal_handler(sig, NULL):
    """
    This is the SIG Interrupt handler.
    """
    files_notfound.value = True
    flag.value = 1


def alarm_handler(sig, NULL):
    """
    This is the SIG Alarm handler. Displays info provided
    by operation_info().
    """
    operation_info()


# Operation Functions

def compress(item):
    """
    This operation compresses the given item.
    :param item: must be a string representing a file in the directory.
    :return: a list with the details about the compress operation.
    """
    start = time.time()                                       # Saves op execution start time
    file_zip = zipfile.ZipFile(item + '.zip', 'w')            # Creates a Zipfile obj needed
    file_zip.write(item, compress_type=zipfile.ZIP_DEFLATED)  # Here is the compress operation
    file_zip.close()                                          # End of compress operation
    end = time.time()

    namesize= len(item)
    elapsed = end - start                                     # Time the process took to compress
    filesize = os.stat(item + ".zip").st_size                 # Gives the file compressed size

    operation = [namesize, item, elapsed, filesize]

    mutex_tval.acquire()
    total_input_size.value += os.stat(item).st_size
    total_output_size.value += filesize
    filesDone.value += 1  # Stores number of sucessfull operations done by processes
    mutex_tval.release()

    if args.verbose:
        print("File compressed by " + str(os.getpid()) + ": " + str(item))

    return operation


def decompress(item):
    """
    This operation decompresses a given file.
    :param item: must be string representing a file in the directory.
    :return: a list with the details about the decompress operation.
    """
    start = time.time()
    file_zip = zipfile.ZipFile(item)            # Here is the decompress operation
    file_zip.extractall("./")                   # Save principals applies
    file_zip.close()
    end = time.time()

    namesize = len(item)
    elapsed = end - start                       # Time the process took to decompress
    filesize = os.stat(item[:-4]).st_size

    operation = [namesize, item, elapsed, filesize]

    mutex_tval.acquire()
    total_input_size.value += os.stat(item).st_size
    total_output_size.value += filesize
    filesDone.value += 1                        # Stores number of sucessfull operations done by processes
    mutex_tval.release()

    if args.verbose:
        print("File decompressed by " + str(os.getpid()) + ": " + str(item) + "  ( in " + str(elapsed) + " seconds )")

    return operation


def pzip():
    """
    This is the main function used as target by the created processes.
    :return:
    """
    p_operations = []
    result = []
    pid = os.getpid()       # Gets the pid of the process
    nfiles = 0
    p_total_output = 0


    # flag = True
    while flag.value == 0:
        try:
            mutex.acquire()
            item = q.get()                                       # Get the next item from the Queue
            mutex.release()

            if item is not None:
                isitemfile = os.path.isfile(item)                # Grabs the file path

                if isitemfile and files_notfound.value == False: # Verifies if the file exists
                    nfiles += 1
                    if args.compress:
                        op = compress(item)
                        p_operations.append(op)
                        p_total_output += os.stat(item + ".zip").st_size

                    if args.decompress:
                        op = decompress(item)
                        p_operations.append(op)
                        p_total_output += os.stat(item[:-4]).st_size
                else:
                    if not isitemfile and args.verbose:
                        print("File not found: " + item)
                    if args.truncate:
                        files_notfound.value = True
                        flag.value = 1
            else:
                flag.value = 1                                   # Exits while loop if a termination sentinel is caught

        except KeyboardInterrupt:
            flag.value = 1

    if args.history:
        result.append(pid)
        result.append(nfiles)
        result.append(p_total_output)
        for op in p_operations:
            result.append(op)

        done_q.put(result)

# History functions

def history(queue):
    """

    :param queue:
    :return:
    """
    with open(args.history, "wb+") as history:
        # Stores the start of the execution of the program, the elapsed time, number of processes and the total size in variables
        start = queue[0]
        elapsed = queue[1]
        nprocesses = queue[2]
        totaloutput = queue[3]

        # Writes this 4 arguments in binary inside the file
        history.write(struct.pack("d", start))
        history.write(struct.pack("d", elapsed))
        history.write(struct.pack("i", nprocesses))
        history.write(struct.pack("i", totaloutput))

        for op in queue:
            if isinstance(op, list):                    # Runs the queue and looks for a list
                # Saves the pid, the number of files and the total size in variables
                pid = op[0]
                nfiles = op[1]
                ptotaloutput = op[2]
                # Writes in binary the pid and the number of files inside the file
                history.write(struct.pack("i", pid))
                history.write(struct.pack("i", nfiles))

                for operation in op:
                    if isinstance(operation, list):

                        namesize = operation[0]         # Stores the number of characters of the name in a variable
                        filename = operation[1]         # Stores the name of the file in a variable
                        totaltime = operation[2]        # Stores the time that the process took to do he's job in a variable
                        size = operation[3]             # Stores the size of the file in a variable

                        history.write(struct.pack("i", namesize))

                        for c in filename:              # Writes a carachter for each character the name of the file as
                            history.write(struct.pack("c", c))

                        history.write(struct.pack("d", totaltime))
                        history.write(struct.pack("i", size))

                history.write(struct.pack("i", ptotaloutput))


###############################################
            ##MAIN PROGRAM##
################################################

parser = argparse.ArgumentParser(description='Compress or Decompress files using Pzip')
group = parser.add_mutually_exclusive_group()
group.add_argument('-c', '--compress', action="store_true", help="Choose to compress file")
group.add_argument('-d', '--decompress', action="store_true", help="Choose to decompress file")
parser.add_argument('-p', '--processes', type=int, nargs="?", default=1, help="Number of processes involved")
parser.add_argument('-t', '--truncate', action="store_true", help="Choose truncate mode")
parser.add_argument('-v', '--verbose', action="store_true", help="Choose verbose mode")
parser.add_argument('-a', '--alarm', type=int, nargs="?", help="Choose to print to stdout execution info")
parser.add_argument('-f', '--history', nargs="?", default="history.txt", help="Save history to file")
parser.add_argument('files', nargs="*", help="Files that are going to be compresses or decompressed")
args = parser.parse_args()


pzip_start = time.time()
file_list = args.files

# Queues
q = Queue()
done_q = Queue()

# Semaphores
mutex = Semaphore(1)
mutex_tval = Semaphore(1)
mutex_end = Semaphore(1)
lock = Lock()

# Shared Variables
flag = Value("i", 0)  # Pzip execution control flag
filesDone = Value("i", 0)  # stores how many files are compressed completly
files_notfound = Value('i', 0)  # Flag to make sure
total_input_size = Value('i', 0)  # Stores total size of the input files in Kb
total_output_size = Value('i', 0)  # Stores total size of the output files in Kb

# Signals & Alarms
signal.signal(signal.SIGINT, signal_handler)

if args.alarm:
    signal.signal(signal.SIGALRM, alarm_handler)
    signal.setitimer(signal.ITIMER_REAL, args.alarm, args.alarm)

# File list to Shared Queue
if args.verbose:
    if len(args.files) == 0:
        file_list = filelist()

for file_name in file_list:
    q.put(file_name)

# Generate processes
processes = []
for n in range(args.processes):
    new_p = Process(target=pzip)
    processes.append(new_p)
    q.put(None)  # send termination sentinel, one for each process

for p in processes:
    p.start()

for p in processes:
    p.join()

# Final Results
pzip_end = time.time()
pzip_totaltime = pzip_end - pzip_start

# History file handling
done_q.put(None)
queue = doneqtolist(pzip_start, pzip_end, args.processes, total_output_size.value)
print(queue)
if args.history:
    history(queue)

if args.verbose:
    print("Files Processed: " + str(filesDone.value) + " files.")
    print("Total input size: " + str(total_input_size.value) + " bytes")
    print("Total output size: " + str(total_output_size.value) + " bytes")
    print("Total time processing: " + str(pzip_totaltime))
