import os, sys, socket
import threading
import queue
import time

#********* CONSTANT VARIABLES *********
BACKLOG = 50            # how many pending connections queue will hold
MAX_DATA_RECV = 4096    # max number of bytes we receive at once
DEBUG = True            # set to True to see the debug msgs

def handle_request(q, queueLock):
  
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

      if (DEBUG):
        print(request)
        #print(first_line)
        #print("URL:", url)

      print("Connect to:", webserver, port)

      try:
        # create a socket to connect to the web server
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((webserver, port))
        s.send(request)         # send request to webserver

        while 1:
          # receive data from web server
          data = s.recv(MAX_DATA_RECV)

          if (len(data) > 0):
            # send to browser
            conn.send(data)
          else:
            break
        s.close()
        conn.close()

      except socket.error as e:
        if s:
          s.close()
        if conn:
          conn.close()
        print("Could not open socket")
        sys.exit(1)

#********* MAIN PROGRAM ***************
def main():

  # check the length of command running
  if (len(sys.argv) < 2):
    print("usage: proxy <port>")
    return sys.stdout
    #exit(1)

  # host and port info.
  host = ""               # blank for localhost
  port = int(sys.argv[1]) # port from argument

  print(port)

  try:
    # create a socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # associate the socket to host and port
    s.bind((host, port))

    # listening
    s.listen(BACKLOG)

    print("A")

  except socket.error as e:
    if e.errno != errno.EINTR:
      s.close()
    print("Could not open socket")
    sys.exit(1)

  q = queue.Queue()
  queueLock = threading.Lock()
  thread = threading.Thread(target = handle_request, args = (q, queueLock))
  thread.start()

  # get the connection from client
  while 1:
    print("B")
    conn, client_addr = s.accept()

    queueLock.acquire()
    q.put((conn, client_addr))
    queueLock.release()

    #handle_request(conn, client_addr)
  s.close()
  thread.join()

if __name__ == '__main__':
  main()

