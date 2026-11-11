# Mede o tempo medio da operacao "list" do GroupMngr.
# Versao com sockets: rodar antes o GroupMngr_sockets.py (copia do original).
from socket import *
import pickle
import time
from constMP import *

N_CALLS = 100

def listWithSockets():
  clientSock = socket(AF_INET, SOCK_STREAM)
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  clientSock.send(pickle.dumps({"op":"list"}))
  msg = clientSock.recv(2048)
  clientSock.close()
  return pickle.loads(msg)

def measure(name, func):
  func()  # primeira chamada fora da medicao
  start = time.time()
  for i in range(N_CALLS):
    func()
  total = time.time() - start
  print(name + ': ' + str(N_CALLS) + ' chamadas em ' + '%.4f' % total + ' s, media de ' + '%.3f' % (total/N_CALLS*1000) + ' ms por chamada')

measure('list com sockets', listWithSockets)
