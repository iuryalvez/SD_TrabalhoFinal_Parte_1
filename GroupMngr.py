from socket import *
import pickle
from constMP import *

port = GROUPMNGR_TCP_PORT
membership = []

def serverLoop():
  serverSock = socket(AF_INET, SOCK_STREAM)
  # Allow immediate reuse of the port after closing
  serverSock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
  serverSock.bind(('0.0.0.0', port))
  serverSock.listen(6)
  while(1):
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(2048)
    req = pickle.loads(msgPack)
    if req["op"] == "register":
      membership.append((req["ipaddr"],req["port"],req["tcpport"]))
      print ('Registered peer: ', req)
    elif req["op"] == "list":
      # cada peer da lista: (ip, porta UDP, porta TCP)
      print ('List of peers sent to server: ', membership)
      conn.send(pickle.dumps(membership))
    elif req["op"] == "unregister":
      # to do
      pass
    elif req["op"] == "stop":
      print("Stopping.")
      serverSock.close()
      break
    else:
      pass # fix (send back an answer in case of unknown op
    conn.close()

serverLoop()
