import queue
import threading
import os, sys, time, socket

BACKLOG = 50
MAX_DATA_RECV = 4096
DEBUG = 1

# WKR_PORT = 2000
# SERVER_PORT = 3000
PROXY_PORTS = [4000, 4001]
RECEIVER_PORTS = [6000, 6001]

# # http_response = "HTTP/1.1 200 OK\nDate: Mon, 27 Jul 2009 12:28:53 GMT\nServer: Apache/2.2.14 (Win32)\nLast-Modified: Wed, 22 Jul 2009 19:15:56 GMT\nContent-Length: 88\nContent-Type: text/html\nConnection: Closed\n\n" + 88*"A"
flask_response = "HTTP/1.1 200 OK\r\nServer: Werkzeug/2.2.2 Python/3.8.10\r\nDate: Mon, 05 Dec 2022 10:41:32 GMT\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: 47\r\nConnection: close\r\n\r\n{\"name\": \"alice\", \"email\": \"alice@outlook.com\"}"

requests_list = list()
queue = list()
conn_dict = dict()

def print_c(*args):
  global DEBUG
  if DEBUG:
      print("[Custom Print] " + " ".join(map(str, args)))

def process_request(req_type, x):
  print("Processing: ", x)
  if req_type == "LONG":
    time.sleep(0.003)
  elif req_type == "MEDIUM":
    time.sleep(0.002)
  elif req_type == "SHORT":
    time.sleep(0.001)
  else:
    print("Not supported req_type: ", req_type)
    exit(1)

def is_overloaded():
  if len(queue) > 100000:
    print("I am overloaded!")
    return True
  else:
    return False

def send_work(proxy_id, proxy_port, host):
  print("Start work stealer client thread")

  while True:
    flag = True
    if is_overloaded():

      last_request = queue.pop()

      sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

      while flag:
        try:
          if proxy_id == 0:
            sock.connect((host, PROXY_PORTS[1]))
            print("connect to port: ", PROXY_PORTS[1])
          else:
            sock.connect((host, PROXY_PORTS[0]))
            print("connect to port: ", PROXY_PORTS[0])
        except socket.error as e:
          if sock:
            sock.close()
            print("Socket error when trying to connect to proxy")
          print("Retrying ...")
        else:
          print("Success")
          flag = False

      data = str(last_request[0]) + "#" + str(last_request[1])
      print("Work steal message: ", data)
      sock.send(data.encode())
      while True:
        response = sock.recv(MAX_DATA_RECV)
        if (len(response) > 0):
          print("Receiver proxy response: ", response.decode())
        else:
          break
      sock.close()
    else:
      time.sleep(0.1)

def receive_work(proxy_id, proxy_port, host):
  print("Start work stealer server thread")
  try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if proxy_id == 0:
      sock.bind((host, PROXY_PORTS[0]))
      print("listen to " + str(PROXY_PORTS[0]))
    else:
      sock.bind((host, PROXY_PORTS[1]))
      print("listen to " + str(PROXY_PORTS[1]))
    sock.listen(BACKLOG)
  except socket.error as e:
    if sock:
      sock.close()
      print("Work stealer server thread error: Closed socket from client to proxy")
    print("Work stealer server error: Could not open socket")
    sys.exit(1)

  while True:
    conn, client_addr = sock.accept() #TODO: bug
    data = conn.recv(MAX_DATA_RECV)
    data = data.decode()
    print("Work stealing: Received data: ", data)

    tokens = data.split("#")
    conn_obj_addr = tokens[0]
    req_type = tokens[1]
    original_proxy_id = tokens[2]
    msg = req_type + "#" + original_proxy_id
    queue.append((conn_obj_addr, msg))

    response = "Proxy " + str(proxy_id) + " received work."
    conn.send(response.encode())
    conn.close()

def receive_back_as_original_proxy(proxy_id, proxy_port, host):
  print("Start completed work receiver server thread")
  try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if proxy_id == 0:
      sock.bind((host, RECEIVER_PORTS[0]))
      print("listen to " + str(RECEIVER_PORTS[0]))
    else:
      sock.bind((host, RECEIVER_PORTS[1]))
      print("listen to " + str(RECEIVER_PORTS[1]))
    sock.listen(BACKLOG)
  except socket.error as e:
    if sock:
      sock.close()
      print("Completed work receiver server thread error: Closed socket from client to proxy")
    print("Completed work receiver server error: Could not open socket")
    sys.exit(1)

  while True:
    conn, client_addr = sock.accept()
    data = conn.recv(MAX_DATA_RECV)
    data = data.decode()
    print("Finished: Received data: ", data)
    response = "Original proxy " + str(proxy_id) + " received completed work."
    conn.send(response.encode())
    conn.close()

    tokens = data.split("#")
    conn_obj_addr = tokens[0]
    conn_ = conn_dict[int(conn_obj_addr)]
    conn_.send(flask_response.encode())
    conn_.close()

def send_back_to_original_proxy(conn_obj_addr_, orig_proxy_id, proxy_port, host):
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  print("send_back_to_original_proxy: ", orig_proxy_id)

  flag = True
  while flag:
    try:
      if orig_proxy_id == "0":
        sock.connect((host, RECEIVER_PORTS[0]))
      else:
        sock.connect((host, RECEIVER_PORTS[1]))
    except socket.error as e:
      if sock:
        sock.close()
        print("A: Socket error when trying to connect to proxy")
      print("A: Retrying ...")
    else:
      flag = False
  print("Before conn_obj_addr_.encode()")
  conn_obj_addr_str_ = str(conn_obj_addr_)
  sock.send(conn_obj_addr_str_.encode())
  print("After conn_obj_addr_.encode()")
  while True:
    data = sock.recv(MAX_DATA_RECV)
    if (len(data) > 0):
      print("Original proxy response: ", data.decode())
    else:
      break
  sock.close()

def handle_request(proxy_id, proxy_port, host, ws_toggle):
  process_count = 1
  while True:
    flag = False
    if (len(queue) > 0):
      conn_obj_addr_, msg_ = queue.pop(0)
      msg_ = msg_.split("#")
      req_type = msg_[0]
      orig_proxy_id = msg_[1]
      print("Dequeued message: conn_obj_addr: " + str(conn_obj_addr_) + ", req_type: " + req_type + ", orig_proxy_id: " + orig_proxy_id)
      flag = True

    if flag:
      if int(orig_proxy_id) == proxy_id:
        conn_ = conn_dict[conn_obj_addr_]
        process_request(req_type, process_count)
        conn_.send(flask_response.encode())
        conn_.close()
        process_count += 1
      elif ws_toggle:
        print("WHY")
        process_request(req_type, process_count)
        send_back_to_original_proxy(conn_obj_addr_, orig_proxy_id, proxy_port, host)

def main():

  if (len(sys.argv) < 7):
    print("usage: proxy <proxy-id> <ip> <port> <proxy-port> <debug-bool> <work steal>")
    print("Closing proxy")
    exit(1)

  proxy_id = int(sys.argv[1])
  host = sys.argv[2]
  port = int(sys.argv[3])
  proxy_port = int(sys.argv[4])

  global DEBUG
  DEBUG = int(sys.argv[5])
  ws_toggle = int(sys.argv[6])
  
  if ws_toggle:
    print("Work stealing is ON.")
  else:
    print("Work stealing is OFF.")
    
  print("Main thread: Proxy ID " + str(proxy_id) + " Host " + host + " Port " + str(port))
  print("Main thread: Initializing ...")

  try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(BACKLOG)
    print("Main thread: Listening on " + str(port))

  except socket.error as e:
    if sock:
      sock.close()
      print("Main thread error: Closed socket from client to proxy")
    else:
      print("Main thread error: Could not open socket")
    print("Main thread error: Closing proxy")
    sys.exit(1)

  process_thread = threading.Thread(target = handle_request, args = (proxy_id, proxy_port, host, ws_toggle))
  work_stealer_client_thread = threading.Thread(target = send_work, args = (proxy_id, proxy_port, host))
  work_stealer_server_thread = threading.Thread(target = receive_work, args = (proxy_id, proxy_port, host))
  completed_work_receiver_thread = threading.Thread(target = receive_back_as_original_proxy, args = (proxy_id, proxy_port, host))
  
  
  process_thread.start()
  if ws_toggle:
    work_stealer_client_thread.start()
    work_stealer_server_thread.start()
    completed_work_receiver_thread.start()

  conn_count = 1
  while True:
    conn, client_addr = sock.accept()
    request = conn.recv(MAX_DATA_RECV)

    if request.decode() == "":
      print("Main thread error: Empty request, closing connection from client")
      conn.close()
      continue

    conn_obj_addr = id(conn)
    print(conn_obj_addr)
    # requests_list.append(conn_obj_addr)
    # print("Number of requests: ", len(requests_list))

    conn_dict[conn_obj_addr] = conn
    msg = "LONG#" + str(proxy_id)
    print("Queuing message: " + msg)

    queue.append((conn_obj_addr, msg))
    conn_count += 1
    print("Main thread: Connection no ", conn_count)
    print("Main thread: Queue size ", len(queue))

  sock.close()
  print("Main thread: Closed socket from client to proxy")

  process_thread.join()
  if ws_toggle:
    work_stealer_client_thread.join()
    work_stealer_server_thread.join()
    completed_work_receiver_thread.join()

if __name__ == '__main__':
  main()
