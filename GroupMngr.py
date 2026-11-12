from concurrent import futures
import threading
import grpc
import mpcomm_pb2
import mpcomm_pb2_grpc
from constMP import *

port = GROUPMNGR_TCP_PORT
membership = []

# O servidor gRPC atende cada chamada em uma thread, entao a lista precisa de lock
lock = threading.Lock()

# Avisa a thread principal que chegou o pedido de parada
stopEvent = threading.Event()

class GroupManager(mpcomm_pb2_grpc.GroupManagerServicer):

  def Register(self, request, context):
    peer = (request.ipaddr, request.udp_port, request.rpc_port)
    with lock:
      # um peer que registra de novo nao entra duas vezes na lista
      if peer not in membership:
        membership.append(peer)
    print ('Registered peer: ', peer)
    return mpcomm_pb2.Reply(text='registered')

  def List(self, request, context):
    reply = mpcomm_pb2.PeerList()
    with lock:
      for m in membership:
        reply.peers.add(ipaddr=m[0], udp_port=m[1], rpc_port=m[2])
      print ('List of peers sent to server: ', membership)
    return reply

  def Stop(self, request, context):
    print("Stopping.")
    stopEvent.set()
    return mpcomm_pb2.Reply(text='stopping')

def serverLoop():
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  mpcomm_pb2_grpc.add_GroupManagerServicer_to_server(GroupManager(), server)
  server.add_insecure_port('0.0.0.0:' + str(port))
  server.start()
  print('Group manager listening on port', port)
  stopEvent.wait()
  # espera 1 segundo para a resposta do Stop sair antes de fechar
  server.stop(1)

serverLoop()
