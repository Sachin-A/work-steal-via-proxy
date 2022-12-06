import random
import queue
import threading
import numpy as np
import os, sys, time, socket

#********* CONSTANT VARIABLES *********
BACKLOG = 50            # how many pending connections queue will hold
MAX_DATA_RECV = 4096    # max number of bytes we receive at once
DEBUG = False           # set to True to see the debug msgs

ports = [2000, 2001]
total_requests = 0
per_proxy = [0, 0]
port_dict = {2000: 0, 2001: 1}

def load_balancer(scheme):
  global per_proxy
  global port_dict

  if scheme == 1:
    ip = ""
    port = random.choice(ports)
    per_proxy[port_dict[port]] += 1
    print(port)

  elif scheme == 2:
    ip = ""
    port = ports[((total_requests + 1) % 2)]
    per_proxy[port_dict[port]] += 1
    print(port)

  elif scheme == 3:
    ip = ""
    print(per_proxy)
    index_min = np.argmin(per_proxy)
    if hasattr(index_min, '__len__') and (not isinstance(index_min, str)):
      port = ports[index_min[0]]
    else:
      port = ports[index_min]
    per_proxy[port_dict[port]] += 1
    print(port)

  return ip, port

def proxy_thread(conn, client_addr, webserver, port):

  global per_proxy
  global port_dict

  request = conn.recv(MAX_DATA_RECV)

  if request.decode() == "":
    print("Handler thread error: Empty request, closing connection from client")
    conn.close()
    return

  print("Connect to: ", webserver, port)

  try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.connect((webserver, port))
    s.send(request)

    while 1:
      data = s.recv(MAX_DATA_RECV)
      if (len(data) > 0):
        conn.send(data)
      else:
        break
    s.close()
    conn.close()
    per_proxy[port_dict[port]] -= 1

  except socket.error as e:
    if s:
      s.close()
      print("Handler thread error: Closed socket from client to load balancer")
    if conn:
      conn.close()
      print("Handler thread error: Closed connection from client to load balancer")
    sys.exit(1)

def main():

  if (len(sys.argv) < 2):
    print("usage: proxy <port>")
    return sys.stdout

  host = ""
  port = int(sys.argv[1])
  scheme = int(sys.argv[2])

  try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, port))
    s.listen(BACKLOG)

  except socket.error as e:
    if s:
      s.close()
      print("Main thread error: Closed socket from client to load balancer")
    else:
      print("Main thread error: Could not open socket")
    print("Main thread error: Closing load balancer")
    sys.exit(1)

  while 1:
    conn, client_addr = s.accept()
    global total_requests
    total_requests += 1
    webserver, port = load_balancer(scheme)
    handler_thread = threading.Thread(target = proxy_thread, args = (conn, client_addr, webserver, port))
    handler_thread.start()

  s.close()
  handler_thread.join()

if __name__ == '__main__':
  main()
