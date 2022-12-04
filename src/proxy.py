import os, sys, socket
import threading
import queue
import time

#********* CONSTANT VARIABLES *********
BACKLOG = 50            # how many pending connections queue will hold
MAX_DATA_RECV = 4096    # max number of bytes we receive at once
DEBUG = 1               # set to True to see the debug msgs

def printCustom(*args):
  global DEBUG
  if DEBUG:
      print("Custom print: " + " ".join(map(str, args)))

def handle_request(q, queueLock):

  x = 1
  while(1):
    flag = False
    queueLock.acquire()
    if (q.qsize() > 0):
      conn, client_addr = q.get()
      if conn:
          flag = True
    queueLock.release()

    if flag:
      
      # get the request from browser
      request = conn.recv(MAX_DATA_RECV)

      # parse the first line
      #first_line = request.split('n')[0]

      # get url
      #url = first_line.split(' ')[1]

      # set webserver hostname and port
      webserver = ""
      port = 3000

      printCustom("Request: ", request)
      #printCustom(first_line)
      #printCustom("URL:", url)

      if request.decode() == "":
          printCustom("Empty request, closing.")
          conn.close()
          continue

      printCustom("Connect to:", webserver, port)

      try:
        # create a socket to connect to the web server
        printCustom("Debug: A")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        printCustom("Debug: B")
        s.connect((webserver, port))
        printCustom("Debug: C")
        s.send(request)         # send request to webserver
        printCustom("Debug: D")

        while 1:
          # receive data from web server
          #printCustom("Debug: E")
          data = s.recv(MAX_DATA_RECV)
          #printCustom("Debug: F")
          if (len(data) > 0):
            # send to browser
            conn.send(data)
            #printCustom("Debug: G")
          else:
            #printCustom("Debug: H")
            break

        #printCustom("Debug: I")
        s.close()
        #printCustom("Request handler: Closed socket to webserver")
        conn.close()
        #printCustom("Request handler: Closed connection from client to proxy")
        print("Processed: ", x)
        x = x + 1

      except socket.error as e:
        if s:
          s.close()
          printCustom("Request handler error: Closed socket to webserver")
        if conn:
          conn.close()
          printCustom("Request handler error: Closed connection from client to proxy")
        printCustom("Could not open socket")
        sys.exit(1)

#********* MAIN PROGRAM ***************
def main():

  # check the length of command running
  if (len(sys.argv) < 3):
    printCustom("usage: proxy <port> <debug-bool>")
    return sys.stdout
    #exit(1)

  # host and port info.
  host = ""               # blank for localhost
  port = int(sys.argv[1]) # port from argument
  global DEBUG
  DEBUG = int(sys.argv[2])

  printCustom("Serving on port:", port)

  try:
    # create a socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # associate the socket to host and port
    s.bind((host, port))

    # listening
    s.listen(BACKLOG)

  except socket.error as e:
    if s:
      s.close()
      printCustom("Main thread error: Closed socket from client to proxy")
    printCustom("Main thread error: Could not open socket")
    sys.exit(1)

  q = queue.Queue()
  queueLock = threading.Lock()
  thread = threading.Thread(target = handle_request, args = (q, queueLock))
  thread.start()

  # get the connection from client
  x = 1
  while 1:
    conn, client_addr = s.accept()
    print(type(conn), type(client_addr))
    print("Connection no:", x)
    queueLock.acquire()
    q.put(conn, client_addr)
    printCustom("Queue size:", q.qsize())
    queueLock.release()
    x = x + 1

  s.close()
  printCustom("Main thread: Closed socket from client to proxy")
  thread.join()

if __name__ == '__main__':
  main()

