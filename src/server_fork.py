import socket

from datetime import datetime
import time

from multiprocessing import Process
from multiprocessing import active_children
from multiprocessing import cpu_count

import config # Creates constants
import connections # Rudimentary protection agains DDoS

def parse_req(buf):
	buf = buf.partition('\r')
	
	if buf[2] == "":
		return -1 # 400 Bad Request
	
	buf = buf[0]
	tokens = buf.replace("?", " ").replace("/", " ").replace("&", " ").split()
	log_file.write(f'Request: {buf}\n')
	log_file.write(f'Tokenized request: {tokens}\n')
	if tokens[0] != "GET":
		return -2 # 405 Method Not Allowed

	tokens.pop(0)
	ano, mes, uf, response_type = "", "", "", "plain"

	for t in tokens:
		if  t.lower().find("uf") != -1:
			uf = t[3::].upper()
		elif t.lower().find("ano") != -1:
			ano = t[4::]
		elif t.lower().find("mes") != -1:
			mes = t[4::].lower().title()
		elif t.lower().find("type") != -1:
			response_type = t[5::]
	
	return (ano, mes, uf, response_type)

def find_in_csv(req):
	valid_lines = []
	
	for l in full_csv_content:
		if (l.find(req[2]) != -1) and (l.find(req[1]) != -1) and (l[0:4] == req[0] or req[0] == ''):
			valid_lines.append(l)
	return valid_lines

'''
def csv_to_pretty_json(header, src):
	header_tokens = header.split(';')
	src_lines = src.replace(";;", "; ;").replace(";;", "; ;").split('\n')
	src_lines.pop() # Last element is empty, because csv file has newline at end
	response = '{\n\t"data": [\n\t\t'

	for l in src_lines:
		response += "{\n\t\t\t"
		line_tokens = l.split(";")
		for i in range(len(line_tokens)):
			if i == len(line_tokens) - 1: # Dont use comma at last element
				response += f'"{header_tokens[i]}": "{line_tokens[i]}"\n\t\t\t'
			else:
				response += f'"{header_tokens[i]}": "{line_tokens[i]}",\n\t\t\t'

		response = response[:-1]
		response += "},\n\t\t"

	response = response[:-4]
	response += "\n\t]\n}"

	return response
'''

def csv_to_json(header, src):
	header_tokens = header.split(';')
	src_lines = src.replace(";;", "; ;").replace(";;", "; ;").split('\n')
	src_lines.pop()
	response = '{"data":['
	
	for l in src_lines:
		response += '{'
		line_tokens = l.split(';')
		for i in range(len(line_tokens)):
			if i == len(line_tokens) - 1: # No comma at last element
				response += f'"{header_tokens[i]}":"{line_tokens[i]}"'
			else:
				response += f'"{header_tokens[i]}":"{line_tokens[i]}",'
		response += "},"
	response = response[:-1] # Remove last comma
	response += "]}"
	
	return response

def handle_client(sock, addr):
	recv_buf = sock.recv(512).decode('ascii')
	log_file.write(f'({datetime.today().strftime('%Y-%m-%d %H:%M:%S')})[+] Received request from {addr[0]}:{addr[1]}, info below:\n')
	t1 = time.time()
	req = parse_req(recv_buf)
	log_file.write(f'Params tuple: {req}\n')
	
	if req == -1:
		data = "HTTP/1.1 400 Bad Request\r\n"
	elif req == -2:
		data = "HTTP/1.1 405 Method Not Allowed\r\n"
	else:
		msg = "".join(find_in_csv(req))
		if req[3].lower() == 'json':
			if req[0] == '' and req[1] == '' and req[2] == '':
				msg = full_json_content
			else:
				msg = csv_to_json(csv_header, msg)
			data = f"HTTP/1.1 200 OK\r\nContent-type: application/json\r\nContent-length: {len(msg)}\r\n\r\n"
		else:
			data = f"HTTP/1.1 200 OK\r\nContent-type: text/plain\r\nContent-length: {len(msg)}\r\n\r\n"
		data += msg
	try:
		sock.sendall(data.encode('ascii'))
	except Exception as e:
		log_file.write(f'({datetime.today().strftime('%Y-%m-%d %H:%M:%S')})[-] Error sending mensage to {addr[0]}:{addr[1]}. {e}\n')

	t2 = time.time()
	
	log_file.write(f'({datetime.today().strftime('%Y-%m-%d %H:%M:%S')})[+] Response sent, time spent: {t2 - t1}s\n')

	return

full_json_file = open(config.FULL_JSON_FILENAME, "r")
full_csv_file = open(config.FULL_CSV_FILENAME, "r")
log_file = open(config.LOG_FILENAME, "a", buffering=1)

full_json_content = full_json_file.read()

full_csv_content = full_csv_file.readlines()
csv_header = full_csv_content[0].strip()
full_csv_content.pop(0)

log_file.write('=' * 80 + '\n')

sock = socket.create_server((config.HOST, config.PORT), family=socket.AF_INET, backlog=config.LISTEN_BACKLOG)
log_file.write(f"({datetime.today().strftime('%Y-%m-%d %H:%M:%S')})[+] Server created, binded at {config.HOST}:{config.PORT} and listening...\n")

full_json_file.close()
full_csv_file.close()

conns = []

t1 = time.time()
while True:
	t2 = time.time()
	if (t2 - t1) > 10: # Run every 10 seconds
		t1 = t2
		connections.cleanup_old_connections(conns)
		log_file.write(f'({datetime.today().strftime('%Y-%m-%d %H:%M:%S')})[+] Cleaning up old connections...\n')
		log_file.write(f'({datetime.today().strftime('%Y-%m-%d %H:%M:%S')})[+] Active connections: {[c.addr for c in conns]}\n')

	client_socket, client_addr = sock.accept()
	client_conn = connections.Connection(client_addr[0])
	check = connections.check_connection(conns, client_conn.addr)
	if check >= 0:
		if check == 1:
			conns.append(client_conn)
		log_file.write(f'({datetime.today().strftime('%Y-%m-%d %H:%M:%S')})[+] Sucessfully connected to client {client_addr[0]}:{client_addr[1]}\n')
		log_file.write(f'({datetime.today().strftime('%Y-%m-%d %H:%M:%S')})[+] Active connections: {[c.addr for c in conns]}\n')
		if len(active_children()) < (cpu_count() - 1):
			proc = Process(target=handle_client, args=(client_socket, client_addr))
			proc.start()
		else:
			handle_client(client_socket, client_addr)
	else:
		log_file.write(f'({datetime.today().strftime('%Y-%m-%d %H:%M:%S')})[-] Denied connection with client {client_addr[0]}:{client_addr[1]}\n')
		client_socket.sendall("HTTP/1.1 429 Too Many Requests\r\n".encode('ascii'))
	client_socket.close()
	log_file.write(f'({datetime.today().strftime('%Y-%m-%d %H:%M:%S')})[+] Ended connection with {client_addr[0]}:{client_addr[1]}\n')
