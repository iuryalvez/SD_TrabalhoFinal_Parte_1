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

	# Compare the lists of messages
	for j in range(0,N_MSGS-1):
		firstMsg = msgs[0][j]
		for i in range(1,N-1):
			if firstMsg != msgs[i][j]:
				unordered = unordered + 1
				break
	
	print ('Found ' + str(unordered) + ' unordered message rounds')

	# limpa os logs para a proxima rodada
	with lock:
		msgs.clear()


# Initiate server:
mainLoop()
