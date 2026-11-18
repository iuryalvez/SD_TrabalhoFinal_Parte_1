PEER_UDP_PORT = 6789
PEER_TCP_PORT = 5679
N = 6   # Number of peers
SERVER_ADDR ='127.0.0.1'
SERVER_PORT = 5678
GROUPMNGR_ADDR = '127.0.0.1'
GROUPMNGR_TCP_PORT = 5680


# Tempo (em segundos) sem confirmacao antes de reenviar uma mensagem
RETRANSMIT_INTERVAL = 0.5

# Fracao dos pacotes UDP recebidos que o peer descarta de proposito, para simular
# perda na rede (0.2 = 20%). Em 0 nao ha perda simulada.
LOSS_RATE = 0.0
