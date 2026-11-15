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

PEERS = []

# Endereco UDP deste peer, como foi registrado no group manager.
# Vai dentro dos ACKs para o remetente saber quem confirmou.
myUdpAddr = None

# Numero da rodada. Cada sinal de inicio do comparison server comeca uma rodada nova.
# As mensagens levam o numero da rodada para que um reenvio atrasado
# de uma rodada nao seja contado na seguinte.
roundNumber = 0

# Estado de cada rodada, indexado pelo numero da rodada. Fica em dicionarios
# porque um peer pode receber o READY de uma rodada antes de ele mesmo
# receber o sinal de inicio dela.
readyFrom = {}   # rodada -> peers que ja mandaram READY
endFrom = {}     # rodada -> peers que ja mandaram a mensagem de fim
received = {}    # rodada -> (remetente, seq) ja recebidos, para descartar duplicadas
logs = {}        # rodada -> mensagens na ordem em que foram recebidas

# Mensagens enviadas que ainda nao foram confirmadas:
# (endereco do destino, rodada, seq) -> [mensagem, hora do ultimo envio]
pending = {}
retransmissions = 0

# A thread que recebe, a que reenvia e a principal mexem nessas estruturas
lock = threading.Lock()

# UDP socket to send and receive data messages.
# O mesmo socket envia e recebe para que o ACK, que e enviado de volta
# para o endereco de origem da mensagem, chegue na thread que recebe.
udpSocket = socket(AF_INET, SOCK_DGRAM)
# Allow immediate reuse of the port after closing
udpSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
udpSocket.bind(('0.0.0.0', PEER_UDP_PORT))

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
  global myUdpAddr
  print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  channel = grpc.insecure_channel(GROUPMNGR_ADDR + ':' + str(GROUPMNGR_TCP_PORT))
  stub = mpcomm_pb2_grpc.GroupManagerStub(channel)
  if myAddr == None:
    ipAddr = get_public_ip()
  else:
    ipAddr = myAddr
  myUdpAddr = (ipAddr, PEER_UDP_PORT)
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

def createRound(r):
  # chamada com o lock ja adquirido
  if r not in readyFrom:
    readyFrom[r] = set()
    endFrom[r] = set()
    received[r] = set()
    logs[r] = []

# Envia uma mensagem e guarda ate chegar o ACK do destino.
# seq identifica a mensagem no ACK: -1 para o READY, 0 a nMsgs-1 para os dados
# e nMsgs para a mensagem de fim.
def sendReliable(msg, peer, seq):
  addr = (peer[0], peer[1])
  msgPack = pickle.dumps(msg)
  with lock:
    pending[(addr, msg[1], seq)] = [msgPack, time.time()]
  udpSocket.sendto(msgPack, addr)

# Thread que reenvia as mensagens sem confirmacao ha mais de RETRANSMIT_INTERVAL
class Retransmitter(threading.Thread):
  def run(self):
    global retransmissions
    while True:
      time.sleep(RETRANSMIT_INTERVAL / 5)
      now = time.time()
      with lock:
        for key in pending:
          if now - pending[key][1] > RETRANSMIT_INTERVAL:
            udpSocket.sendto(pending[key][0], key[0])
            pending[key][1] = now
            retransmissions = retransmissions + 1

# Thread que recebe todas as mensagens UDP. Fica rodando enquanto o processo
# existir, para continuar confirmando reenvios mesmo depois do fim da rodada.
class MsgHandler(threading.Thread):
  def __init__(self, sock):
    threading.Thread.__init__(self)
    self.sock = sock

  def run(self):
    print('Handler is ready.')
    while True:
      (msgPack, addr) = self.sock.recvfrom(1024)   # receive data from client
      msg = pickle.loads(msgPack)

      if msg[0] == 'ACK':
        # ('ACK', rodada, endereco de quem confirmou, seq)
        with lock:
          pending.pop((msg[2], msg[1], msg[3]), None)
        continue

      r = msg[1]
      with lock:
        createRound(r)
        if msg[0] == 'READY':
          # ('READY', rodada, remetente)
          seq = -1
          if msg[2] not in readyFrom[r]:
            readyFrom[r].add(msg[2])
            print('--- Handshake received: ', msg[2])
        elif msg[0] == 'DATA':
          # ('DATA', rodada, remetente, seq)
          seq = msg[3]
          if (msg[2], seq) not in received[r]:
            received[r].add((msg[2], seq))
            logs[r].append((msg[2], seq))
            print('Message ' + str(seq) + ' from process ' + str(msg[2]))
        elif msg[0] == 'END':
          # ('END', rodada, remetente, nMsgs): o remetente nao tem mais mensagens
          seq = msg[3]
          endFrom[r].add(msg[2])

      # Confirma sempre, mesmo uma duplicada: o ACK anterior pode ter se perdido
      ack = ('ACK', r, myUdpAddr, seq)
      self.sock.sendto(pickle.dumps(ack), addr)

# Function to wait for start signal from comparison server:
def waitToStart():
  # fica bloqueado ate o servico gRPC receber uma chamada Start
  (myself, nMsgs) = startQueue.get()
  return (myself,nMsgs)

# Send the list of messages to the server (using RPC) for comparison
def sendLogToServer(myself, logList):
  print('Sending the list of messages to the server for comparison...')
  channel = grpc.insecure_channel(SERVER_ADDR + ':' + str(SERVER_PORT))
  stub = mpcomm_pb2_grpc.ComparisonServerStub(channel)
  log = mpcomm_pb2.MsgLog(peer_id=myself)
  for m in logList:
    log.msgs.add(sender=m[0], seq=m[1])
  stub.SendLog(log)
  channel.close()

# From here, code is executed when program starts:
registerWithGroupManager()

# As duas threads sao daemon para o processo terminar quando a principal terminar
msgHandler = MsgHandler(udpSocket)
msgHandler.daemon = True
msgHandler.start()
retransmitter = Retransmitter()
retransmitter.daemon = True
retransmitter.start()

while 1:
  print('Waiting for signal to start...')
  (myself, nMsgs) = waitToStart()
  print('I am up, and my ID is: ', str(myself))

  if nMsgs == 0:
    print('Terminating.')
    # espera 1 segundo para a resposta do Start sair antes de fechar
    peerServer.stop(1)
    exit(0)

  roundNumber = roundNumber + 1
  retransmissions = 0
  with lock:
    createRound(roundNumber)

  PEERS = getListOfPeers()

  # Send handshakes. Cada READY e reenviado ate o destino confirmar.
  for addrToSend in PEERS:
    print('Sending handshake to ', addrToSend)
    sendReliable(('READY', roundNumber, myself), addrToSend, -1)

  # Wait until handshakes are received from all other processes
  # (to make sure that all processes are synchronized before they start exchanging messages)
  while len(readyFrom[roundNumber]) < N:
    time.sleep(0.01)
  print('Main Thread: Received all handshakes.')

  # Send a sequence of data messages to all other processes
  for msgNumber in range(0, nMsgs):
    # Wait some random time between successive messages
    time.sleep(random.randrange(10,100)/1000)
    for addrToSend in PEERS:
      sendReliable(('DATA', roundNumber, myself, msgNumber), addrToSend, msgNumber)
    print('Sent message ' + str(msgNumber))

  # Tell all processes that I have no more messages to send
  for addrToSend in PEERS:
    sendReliable(('END', roundNumber, myself, nMsgs), addrToSend, nMsgs)

  # A rodada termina quando todos avisaram o fim e chegaram todas as mensagens.
  # Sem as duas condicoes, um fim que chega antes de um reenvio encerraria cedo.
  while len(endFrom[roundNumber]) < N or len(logs[roundNumber]) < N * nMsgs:
    time.sleep(0.01)
  print('Received all messages. Retransmissions in this round: ' + str(retransmissions))

  logList = logs[roundNumber]

  # Write log file
  logFile = open('logfile'+str(myself)+'.log', 'w')
  logFile.writelines(str(logList))
  logFile.close()

  sendLogToServer(myself, logList)
