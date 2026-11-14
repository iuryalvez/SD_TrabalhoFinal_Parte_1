from constMP import *
import time
import sys
import threading
from concurrent import futures
import grpc
import mpcomm_pb2
import mpcomm_pb2_grpc

# Logs recebidos dos peers na rodada atual
msgs = [] # each msg is a list of tuples (with the original messages received by the peer processes)
lock = threading.Lock()

# Servico gRPC que recebe o log de cada peer
class ComparisonServer(mpcomm_pb2_grpc.ComparisonServerServicer):
	def SendLog(self, request, context):
		log = []
		for m in request.msgs:
			log.append((m.sender, m.seq))
		with lock:
			msgs.append(log)
		print ('Received log from peer', request.peer_id)
		return mpcomm_pb2.Reply(text='log received')

server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
mpcomm_pb2_grpc.add_ComparisonServerServicer_to_server(ComparisonServer(), server)
server.add_insecure_port('0.0.0.0:' + str(SERVER_PORT))
server.start()

def mainLoop():
	cont = 1
	while 1:
		nMsgs = promptUser()
		channel = grpc.insecure_channel(GROUPMNGR_ADDR + ':' + str(GROUPMNGR_TCP_PORT))
		stub = mpcomm_pb2_grpc.GroupManagerStub(channel)
		reply = stub.List(mpcomm_pb2.Empty())
		peerList = []
		for p in reply.peers:
			peerList.append((p.ipaddr, p.udp_port, p.rpc_port))
		print("List of Peers: ", peerList)
		startPeers(peerList,nMsgs)
		if nMsgs != 0:
			print('Now, wait for the message logs from the communicating peers...')
			waitForLogsAndCompare(nMsgs)
		else:
			print('Stopping.')
			server.stop(1)

			# Tell group manager to stop
			stub.Stop(mpcomm_pb2.Empty())
			channel.close()
			break

def promptUser():
	nMsgs = int(input('Enter the number of messages for each peer to send (0 to terminate)=> '))
	return nMsgs

def startPeers(peerList,nMsgs):
	# Connect to each of the peers and send the 'initiate' signal:
	peerNumber = 0
	for peer in peerList:
		# peer = (ip, porta UDP, porta do servico gRPC)
		channel = grpc.insecure_channel(peer[0] + ':' + str(peer[2]))
		stub = mpcomm_pb2_grpc.PeerStub(channel)
		reply = stub.Start(mpcomm_pb2.StartRequest(peer_id=peerNumber, n_msgs=nMsgs))
		print(reply.text)
		channel.close()
		peerNumber = peerNumber + 1

def waitForLogsAndCompare(N_MSGS):
	# Wait until the gRPC service has received the logs from all the peer processes
	while len(msgs) < N:
		time.sleep(0.1)

	unordered = 0

	# Cada peer recebe N_MSGS mensagens de cada um dos N peers (inclusive dele mesmo).
	# Se alguma mensagem se perdeu, os logs podem ter tamanhos diferentes,
	# entao a comparacao por posicao vai ate o menor log.
	shortest = len(msgs[0])
	for i in range(1,N):
		if len(msgs[i]) < shortest:
			shortest = len(msgs[i])

	# Compare the lists of messages (todas as posicoes e todos os peers)
	for j in range(0,shortest):
		firstMsg = msgs[0][j]
		for i in range(1,N):
			if firstMsg != msgs[i][j]:
				unordered = unordered + 1
				break

	print ('Found ' + str(unordered) + ' unordered message rounds')

	# Verifica cada log separado: mensagens que faltaram e ordem FIFO por remetente
	totalMissing = 0
	totalFifoErrors = 0
	for i in range(0,N):
		lastSeq = {}    # ultimo numero de sequencia recebido de cada remetente
		received = {}   # numeros de sequencia recebidos de cada remetente
		fifoErrors = 0
		for (sender, seq) in msgs[i]:
			if sender in lastSeq and seq < lastSeq[sender]:
				fifoErrors = fifoErrors + 1
			lastSeq[sender] = seq
			if sender not in received:
				received[sender] = set()
			received[sender].add(seq)
		missing = 0
		for sender in range(0,N):
			if sender in received:
				missing = missing + N_MSGS - len(received[sender])
			else:
				missing = missing + N_MSGS
		print ('Log ' + str(i) + ': ' + str(len(msgs[i])) + ' messages, ' + str(missing) + ' missing, ' + str(fifoErrors) + ' FIFO violations')
		totalMissing = totalMissing + missing
		totalFifoErrors = totalFifoErrors + fifoErrors

	print ('Total: ' + str(totalMissing) + ' missing messages and ' + str(totalFifoErrors) + ' FIFO violations')

	# limpa os logs para a proxima rodada
	with lock:
		msgs.clear()


# Initiate server:
mainLoop()
