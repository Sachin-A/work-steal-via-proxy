import queue
import threading
import os, sys, time, socket
import numpy as np
import random

BACKLOG = 50
MAX_DATA_RECV = 4096
DEBUG = 1

UPDATE_PORTS = [4400, 4401]
PROXY_PORTS = [4000, 4001]
RECEIVER_PORTS = [6000, 6001]

flask_response = "HTTP/1.1 200 OK\r\nServer: Werkzeug/2.2.2 Python/3.8.10\r\nDate: Mon, 05 Dec 2022 10:41:32 GMT\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: 47\r\nConnection: close\r\n\r\n{\"name\": \"alice\", \"email\": \"alice@outlook.com\"}"

requests_list = list()
queue = list()
conn_dict = dict()
steal_count = 0
req_type_dict = {0: "LONG", 1: "MEDIUM", 2: "SHORT"}
mean_processing_time = {"LONG":3, "MEDIUM":2, "SHORT":1}
queue_states = dict()
first_wrk_request = False

def print_c(*args):
  global DEBUG
  if DEBUG:
      print("[Custom Print] " + " ".join(map(str, args)))

def sample_normal_dist(mean):
  std = 0.1
  return np.random.normal(mean, std, 1)

def process_request(req_type, x):
  if req_type not in mean_processing_time:
    print("ERROR: Couldn't find req_type in dict: ", req_type)
  sample = sample_normal_dist(mean_processing_time[req_type])[0]
  print("sample: ", sample)
  print("Processing: " + str(x) + ", duration: " + str(round(sample, 3)) + " ms")
  time.sleep(sample*0.001)

def is_overloaded():
  if len(queue) > 5:
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
      global steal_count
      steal_count += 1
      while True:
        response = sock.recv(MAX_DATA_RECV)
        if (len(response) > 0):
          print("Receiver proxy response: ", response.decode())
        else:
          break
      sock.close()
    else:
      time.sleep(0.01)

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
        conn_ = conn_dict[int(conn_obj_addr_)]
        process_request(req_type, process_count)
        conn_.send(flask_response.encode())
        conn_.close()
        process_count += 1
      elif ws_toggle:
        process_request(req_type, process_count)
        send_back_to_original_proxy(conn_obj_addr_, orig_proxy_id, proxy_port, host)


def update_state(d_):
  d_ = d_.split("#") # src_id#queue_size#est_q_time
  src_id = d_[0]
  q_size = d_[1]
  est_q_time = d_[2]
  # if src_id in queue_states:
  #   old_q_size = queue_states[src_id][0]
  #   old_est_q_time = queue_states[src_id][1]
  # else:
  #   old_q_size = 0
  #   old_est_q_time = 0
  queue_states[src_id] = [q_size, est_q_time]
  # if src_id in queue_states:
      # DEBUG Print
  #   print("Update queue state: proxy " + str(src_id) + ", queue size(" + str(old_q_size) + "->" + str(q_size) + "), estimated_queue_time(" + str(old_est_q_time) + "->" + str(est_q_time) + ")")


def recv_queue_state(my_id):
  print("Start recv_state thread")
  host = ""
  try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, UPDATE_PORTS[int(my_id)]))
    print("listen to " + str(UPDATE_PORTS[int(my_id)]))
    sock.listen(BACKLOG)
  except socket.error as e:
    if sock:
      sock.close()
      print("recv_state thread error: Closed socket from client to proxy")
    print("recv_state error: Could not open socket")
    sys.exit(1)

  while True:
    conn, client_addr = sock.accept()
    data = conn.recv(MAX_DATA_RECV)
    data = data.decode()
    update_state(data)
    response = "Proxy " + str(my_id) + " gets updated."
    conn.send(response.encode())
    conn.close()


def calc_est_queue_time():
  est_q_time = 0
  for req in queue:
    req_type = req[1].split("#")[0]
    est_q_time += mean_processing_time[req_type]
  # DEBUG Print
  # print("Est queue time: ", est_q_time)
  return est_q_time

def tcp_send(port_, data_):
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  sock.connect(("", port_))
  # DEBUG Print
  # print("connect to port: ", port_)
  sock.send(data_.encode())
  while True:
    response = sock.recv(MAX_DATA_RECV)
    if (len(response) > 0):
      # DEBUG Print
      # print("Getter proxy response: ", response.decode())
      break
  sock.close()

def push_queue_state(my_id):
  update_interval = 0.005
  print("Start push_queue_state thread")
  global first_wrk_request
  while True:
    if first_wrk_request:
      data = str(my_id) + "#" + str(len(queue)) + "#" + str(calc_est_queue_time()) # src_id#queue_size#est_queue_time
      if my_id == 0:
        tcp_send(UPDATE_PORTS[1], data)
      elif my_id == 1:
        tcp_send(UPDATE_PORTS[0], data)
      else:
        print("error my id(proxy id)")
        exit(-1)
    else:
      time.sleep(0.1)
    time.sleep(update_interval)

def set_avg_latency(req_type, avg_latency):
  mean_processing_time[req_type] = avg_latency
  print("Set " + req_type + " avg latency: " + str(avg_latency))

def main():
  if (len(sys.argv) < 10):
    print("usage: proxy <proxy-id> <ip> <port> <proxy-port> <debug-bool> <work steal> <long> <medium> <short>")
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

  long_avg_latency = int(sys.argv[7])
  medium_avg_latency = int(sys.argv[8])
  short_avg_latency = int(sys.argv[9])
  set_avg_latency("LONG", long_avg_latency)
  set_avg_latency("MEDIUM", medium_avg_latency)
  set_avg_latency("SHORT", short_avg_latency)

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
  recv_queue_state_thread = threading.Thread(target = recv_queue_state, args = (proxy_id,))
  push_queue_state_thread = threading.Thread(target = push_queue_state, args = (proxy_id,))

  process_thread.start()
  if ws_toggle:
    work_stealer_client_thread.start()
    work_stealer_server_thread.start()
    completed_work_receiver_thread.start()
    push_queue_state_thread.start()
    recv_queue_state_thread.start()

  conn_count = 1
  while True:
    conn, client_addr = sock.accept()
    global first_wrk_request
    first_wrk_request = True
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
    req_type = req_type_dict[random.randint(0, 2)]
    msg = req_type + "#" + str(proxy_id)
    print("Queuing message: " + msg)

    queue.append((conn_obj_addr, msg))
    conn_count += 1
    print("Main thread: Connection no ", conn_count)
    print("Main thread: Queue size ", len(queue))
    print("Main thread: Steal count ", str(steal_count))

  sock.close()
  print("Main thread: Closed socket from client to proxy")

  process_thread.join()
  if ws_toggle:
    work_stealer_client_thread.join()
    work_stealer_server_thread.join()
    completed_work_receiver_thread.join()
    push_queue_state_thread.join()
    recv_queue_state_thread.join()

if __name__ == '__main__':
  main()
