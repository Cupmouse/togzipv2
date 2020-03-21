from subprocess import call
import sys
import os
import multiprocessing
import queue

path = sys.argv[1]
step = int(sys.argv[2])
files = os.listdir(path)
q = multiprocessing.Queue(0)

for file in files:
    q.put('pigz -dc %s | ./togzipv2 %s' % (os.path.join(path, file), file[:file.find('.')]))

def go():
	while True:
		try:
			command = q.get(False)
			print(command)
			os.system(command)
		except queue.Empty:
			return

for i in range(step):
	multiprocessing.Process(target=go).start()
