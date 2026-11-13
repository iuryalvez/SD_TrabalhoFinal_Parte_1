from socket  import *
from constMP import * #-
import threading
import random
import time
import pickle
import sys
import queue
from concurrent import futures
import grpc
import mpcomm_pb2
import mpcomm_pb2_grpc
from requests import get

# Uso: python3 peerCommunicatorUDP.py [ip porta_udp porta_tcp]
# Com os argumentos da pra rodar varios peers na mesma maquina.
# Sem argumentos usa o IP publico e as portas do constMP.py.
myAddr = None
if len(sys.argv) == 4:
  myAddr = sys.argv[1]
  PEER_UDP_PORT = int(sys.argv[2])
  PEER_TCP_PORT = int(sys.argv[3])

#handShakes = [] # not used; only if we need to check whose handshake is missing

# Counter to make sure we have received handshakes from all other processes
handShakeCount = 0

PEERS = []

# UDP sockets to send and receive data messages:
# Create send socket
sendSocket = socket(AF_INET, SOCK_DGRAM)
#Create and bind receive socket
recvSocket = socket(AF_INET, SOCK_DGRAM)
# Allow immediate reuse of the port after closing
recvSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# Fila onde o servico gRPC coloca o sinal de inicio recebido do comparison server
startQueue = queue.Queue()

# Servico gRPC do peer: recebe o sinal de inicio do comparison server
class Peer(mpcomm_pb2_grpc.PeerServicer):
  def Start(self, request, context):
    startQueue.put((request.peer_id, request.n_msgs))
    return mpcomm_pb2.Reply(text='Peer process '+str(request.peer_id)+' started.')

peerServer = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
mpcomm_pb2_grpc.add_PeerServicer_to_server(Peer(), peerServer)
peerServer.add_insecure_port('0.0.0.0:' + str(PEER_TCP_PORT))
peerServer.start()


def get_public_ip():
  ipAddr = get('https://api.ipify.org').content.decode('utf8')
  print('My public IP address is: {}'.format(ipAddr))
  return ipAddr

# Function to register this peer with the group manager
def registerWithGroupManager():
  print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  channel = grpc.insecure_channel(GROUPMNGR_ADDR + ':' + str(GROUPMNGR_TCP_PORT))
  stub = mpcomm_pb2_grpc.GroupManagerStub(channel)
  if myAddr == None:
    ipAddr = get_public_ip()
  else:
    ipAddr = myAddr
  req = mpcomm_pb2.PeerInfo(ipaddr=ipAddr, udp_port=PEER_UDP_PORT, rpc_port=PEER_TCP_PORT)
  print ('Registering with group manager: ', (ipAddr, PEER_UDP_PORT, PEER_TCP_PORT))
  stub.Register(req)
  channel.close()

def getListOfPeers():
  print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  channel = grpc.insecure_channel(GROUPMNGR_ADDR + ':' + str(GROUPMNGR_TCP_PORT))
  stub = mpcomm_pb2_grpc.GroupManagerStub(channel)
  print ('Getting list of peers from group manager')
  reply = stub.List(mpcomm_pb2.Empty())
  # cada peer da lista: (ip, porta UDP, porta TCP)
  PEERS = []
  for p in reply.peers:
    PEERS.append((p.ipaddr, p.udp_port, p.rpc_port))
  print ('Got list of peers: ', PEERS)
  channel.close()
  return PEERS

class MsgHandler(threading.Thread):
  def __init__(self, sock):
    threading.Thread.__init__(self)
    self.sock = sock

  def run(self):
    print('Handler is ready. Waiting for the handshakes...')
    
    #global handShakes
    global handShakeCount
    
    logList = []
    
    # Wait until handshakes are received from all other processes
    # (to make sure that all processes are synchronized before they start exchanging messages)
    while handShakeCount < N:
      msgPack = self.sock.recv(1024)
      msg = pickle.loads(msgPack)
      #print ('########## unpickled msgPack: ', msg)
      if msg[0] == 'READY':

        # To do: send reply of handshake and wait for confirmation

        handShakeCount = handShakeCount + 1
        #handShakes[msg[1]] = 1
        print('--- Handshake received: ', msg[1])

    print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

    stopCount=0 
    while True:                
      msgPack = self.sock.recv(1024)   # receive data from client
      msg = pickle.loads(msgPack)
      if msg[0] == -1:   # count the 'stop' messages from the other processes
        stopCount = stopCount + 1
        if stopCount == N:
          break  # stop loop when all other processes have finished
      else:
        print('Message ' + str(msg[1]) + ' from process ' + str(msg[0]))
        logList.append(msg)
        
    # Write log file
    logFile = open('logfile'+str(myself)+'.log', 'w')
    logFile.writelines(str(logList))
    logFile.close()
    
    # Send the list of messages to the server (using RPC) for comparison
    print('Sending the list of messages to the server for comparison...')
    channel = grpc.insecure_channel(SERVER_ADDR + ':' + str(SERVER_PORT))
    stub = mpcomm_pb2_grpc.ComparisonServerStub(channel)
    log = mpcomm_pb2.MsgLog(peer_id=myself)
    for m in logList:
      log.msgs.add(sender=m[0], seq=m[1])
    stub.SendLog(log)
    channel.close()
    
    # Reset the handshake counter
    handShakeCount = 0

    exit(0)

# Function to wait for start signal from comparison server:
def waitToStart():
  # fica bloqueado ate o servico gRPC receber uma chamada Start
  (myself, nMsgs) = startQueue.get()
  return (myself,nMsgs)

# From here, code is executed when program starts:
registerWithGroupManager()
while 1:
  print('Waiting for signal to start...')
  (myself, nMsgs) = waitToStart()
  print('I am up, and my ID is: ', str(myself))

  if nMsgs == 0:
    print('Terminating.')
    # espera 1 segundo para a resposta do Start sair antes de fechar
    peerServer.stop(1)
    exit(0)

  # Create receiving message handler
  msgHandler = MsgHandler(recvSocket)
  msgHandler.start()
  print('Handler started')

  PEERS = getListOfPeers()
  
  # Send handshakes
  # To do: Must continue sending until it gets a reply from each process
  #        Send confirmation of reply
  for addrToSend in PEERS:
    print('Sending handshake to ', addrToSend)
    msg = ('READY', myself)
    msgPack = pickle.dumps(msg)
    sendSocket.sendto(msgPack, (addrToSend[0],addrToSend[1]))
    #data = recvSocket.recvfrom(128) # Handshadke confirmations have not yet been implemented

  print('Main Thread: Sent all handshakes. handShakeCount=', str(handShakeCount))

  while (handShakeCount < N):
    pass  # find a better way to wait for the handshakes

  # Send a sequence of data messages to all other processes 
  for msgNumber in range(0, nMsgs):
    # Wait some random time between successive messages
    time.sleep(random.randrange(10,100)/1000)
    msg = (myself, msgNumber)
    msgPack = pickle.dumps(msg)
    for addrToSend in PEERS:
      sendSocket.sendto(msgPack, (addrToSend[0],addrToSend[1]))
      print('Sent message ' + str(msgNumber))

  # Tell all processes that I have no more messages to send
  for addrToSend in PEERS:
    msg = (-1,-1)
    msgPack = pickle.dumps(msg)
    sendSocket.sendto(msgPack, (addrToSend[0],addrToSend[1]))
