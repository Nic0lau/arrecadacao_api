import socket
import time
from multiprocessing import Process
from multiprocessing import active_children
from multiprocessing import cpu_count

def send_msg():
	sock = socket.create_connection(('127.0.0.1', 42069))
	sock.sendall('GET /type=json HTTP/1.1\r\n'.encode('ascii'))
	buf = ""
	t1 = time.time()
	while True:
		resp = sock.recv(16384).decode('ascii')
		if not resp:
			break
		else:
			buf += resp
	t2 = time.time()
	print(f"{t2-t1}")
	sock.close()

	return

for i in range(10000):
#while True:
	if len(active_children()) < (cpu_count() - 1):
		p = Process(target=send_msg, args=())
		p.start()
	else:
		send_msg()

#cleanBuf = buf.split('\n', 4)[4]
#js_buf = json.loads(cleanBuf)
#print(json.dumps(js_buf["data"][0], indent=2))
