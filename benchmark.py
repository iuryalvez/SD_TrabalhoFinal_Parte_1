# Mede o tempo medio da operacao "list" do GroupMngr.
# Uso: python3 benchmark.py sockets   (rodar antes o GroupMngr_sockets.py, copia do original)
#      python3 benchmark.py grpc      (rodar antes o GroupMngr.py)
from socket import *
import pickle
import time
import sys
import grpc
import mpcomm_pb2
import mpcomm_pb2_grpc
from constMP import *

N_CALLS = 100

def listWithSockets():
  clientSock = socket(AF_INET, SOCK_STREAM)
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  clientSock.send(pickle.dumps({"op":"list"}))
  msg = clientSock.recv(2048)
  clientSock.close()
  return pickle.loads(msg)

# O canal e o stub sao criados uma vez so, como no uso normal do gRPC
def listWithGrpc():
  reply = stub.List(mpcomm_pb2.Empty())
  return reply.peers

# Igual a versao com sockets: abre uma conexao nova a cada chamada
def listWithGrpcNewChannel():
  channel = grpc.insecure_channel(GROUPMNGR_ADDR + ':' + str(GROUPMNGR_TCP_PORT))
  stub = mpcomm_pb2_grpc.GroupManagerStub(channel)
  reply = stub.List(mpcomm_pb2.Empty())
  channel.close()
  return reply.peers

def measure(name, func):
  func()  # primeira chamada fora da medicao
  start = time.time()
  for i in range(N_CALLS):
    func()
  total = time.time() - start
  print(name + ': ' + str(N_CALLS) + ' chamadas em ' + '%.4f' % total + ' s, media de ' + '%.3f' % (total/N_CALLS*1000) + ' ms por chamada')

if len(sys.argv) == 2 and sys.argv[1] == 'grpc':
  channel = grpc.insecure_channel(GROUPMNGR_ADDR + ':' + str(GROUPMNGR_TCP_PORT))
  stub = mpcomm_pb2_grpc.GroupManagerStub(channel)
  measure('list com gRPC (mesmo canal)', listWithGrpc)
  measure('list com gRPC (canal novo a cada chamada)', listWithGrpcNewChannel)
else:
  measure('list com sockets', listWithSockets)
