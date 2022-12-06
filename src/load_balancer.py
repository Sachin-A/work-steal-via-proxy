import queue
import threading
import os, sys, time, socket

#********* CONSTANT VARIABLES *********
BACKLOG = 50            # how many pending connections queue will hold
MAX_DATA_RECV = 4096    # max number of bytes we receive at once
DEBUG = False           # set to True to see the debug msgs

def load_balancer(scheme):
    ip = ""
    port = 2000
    return ip, port

def proxy_thread(conn, client_addr):

  request = conn.recv(MAX_DATA_RECV)

  if request.decode() == "":
    print("Handler thread error: Empty request, closing connection from client")
    conn.close()
    return

  webserver, port = load_balancer(0)
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
    handler_thread = threading.Thread(target = proxy_thread, args = (conn, client_addr))
    handler_thread.start()

  s.close()
  handler_thread.join()

if __name__ == '__main__':
  main()
