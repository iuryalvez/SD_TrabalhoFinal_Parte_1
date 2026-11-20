# MPComm - Comunicação multicast com RPC, tratamento de perdas e ordem FIFO

Partimos do MPComm (https://github.com/professorfabio/MPComm), um sistema em que um grupo de processos (peers) envia mensagens por multicast para todos os outros, sem nenhuma coordenação. Ele mostra o problema da escalabilidade geográfica: com os peers espalhados por regiões diferentes da internet, cada peer recebe as mensagens em uma ordem diferente.

Nesta versão fizemos duas mudanças nos protocolos de comunicação:

1. Todas as interações cliente-servidor, que usavam sockets TCP com `pickle`, passaram a usar RPC com gRPC.
2. O multicast continua em sockets UDP, mas agora trata a perda de mensagens (confirmação e reenvio) e entrega as mensagens de cada remetente na ordem em que foram enviadas (ordem FIFO).

## Funcionalidades

- Registro dos peers em um gerenciador de grupo e consulta da lista de peers, por RPC.
- Início de cada rodada de mensagens pelo servidor de comparação, por RPC.
- Multicast UDP de uma sequência de mensagens de cada peer para todos os peers.
- Confirmação (ACK) de cada mensagem UDP e reenvio das que não forem confirmadas.
- Descarte de mensagens duplicadas, que aparecem quando um ACK se perde e a mensagem é reenviada.
- Entrega em ordem FIFO: as mensagens de um mesmo remetente entram no log na ordem em que ele enviou.
- Perda simulada configurável, para demonstrar o reenvio na máquina local, onde não há perda real.
- Envio do log de cada peer ao servidor de comparação, por RPC, e um relatório com rodadas fora de ordem, mensagens que faltaram e violações de FIFO.

## Arquitetura

São três tipos de processo:

```
                 Register / List / Stop (gRPC)
   peers  ──────────────────────────────────────►  GroupMngr.py
     ▲  │                                              ▲
     │  │ SendLog (gRPC)                               │ List / Stop (gRPC)
     │  ▼                                              │
     │  comparisonServer.py ───────────────────────────┘
     │          │
     └──────────┘ Start (gRPC)

   peer ◄────── multicast UDP (READY, DATA, END, ACK) ──────► peer
```

- `GroupMngr.py`: gerenciador de grupo. Guarda o endereço de cada peer (IP, porta UDP e porta do serviço gRPC) e devolve a lista quando pedida.
- `comparisonServer.py`: pergunta quantas mensagens cada peer deve enviar, busca a lista de peers, manda o sinal de início para cada um, espera os logs e compara.
- `peerCommunicatorUDP.py`: o peer. Registra-se no gerenciador, espera o sinal de início, troca as mensagens com os outros peers por UDP e manda o seu log ao servidor de comparação.

O contrato dos três serviços está em `mpcomm.proto`:

- `GroupManager` (no `GroupMngr.py`): `Register`, `List` e `Stop`.
- `Peer` (em cada peer): `Start`, o sinal de início da rodada, com o identificador do peer e o número de mensagens.
- `ComparisonServer` (no `comparisonServer.py`): `SendLog`, por onde cada peer entrega a lista de mensagens recebidas.

Arquivos do repositório:

- `constMP.py`: endereços, portas, número de peers (`N`), intervalo de reenvio e taxa de perda simulada.
- `mpcomm.proto`, `mpcomm_pb2.py` e `mpcomm_pb2_grpc.py`: contrato gRPC e o código gerado a partir dele.
- `GroupMngr_sockets.py`: cópia do gerenciador original com sockets, usada só na medição.
- `benchmark.py` e `medicoes.txt`: medição do tempo da chamada `list` nas duas versões e as saídas obtidas.

## Detalhes de implementação

### a) Sockets substituídos por RPC

No código base, cada interação cliente-servidor abria um socket TCP, mandava um dicionário serializado com `pickle` (por exemplo `{"op":"list"}`) e lia a resposta com `recv` de tamanho fixo. O servidor decidia o que fazer olhando o campo `"op"`.

Com gRPC, cada operação virou um método do contrato `mpcomm.proto`, e o código de comunicação é gerado pelo `grpc_tools.protoc`. Do lado do cliente, buscar a lista de peers passou a ser `stub.List(mpcomm_pb2.Empty())`. Do lado do servidor, cada operação é um método de uma classe (`GroupManager`, `Peer`, `ComparisonServer`). É o funcionamento descrito no Cap. 4 do livro *Distributed Systems* (van Steen e Tanenbaum, seção 4.2): o stub do cliente empacota os parâmetros numa mensagem e espera a resposta, e o stub do servidor desempacota e chama o procedimento.

Algumas mudanças foram necessárias por causa do modelo do gRPC:

- O servidor gRPC atende cada chamada em uma thread do seu pool. Por isso a lista de peers do `GroupMngr.py` e a lista de logs do `comparisonServer.py` são protegidas por um `threading.Lock`.
- No peer, o sinal de início chega na thread do gRPC, mas quem conduz a rodada é a thread principal. O método `Start` coloca o sinal numa `queue.Queue`, e a função `waitToStart` espera nessa fila.
- O `comparisonServer.py` deixou de aceitar conexões em um laço e passou a ser um servidor gRPC com o método `SendLog`. A função `waitForLogsAndCompare` espera até a lista ter os `N` logs.
- O `Register` não duplica um peer que se registra de novo.

### b) Perda de mensagens e ordem FIFO no multicast

O multicast continua em UDP, que não garante entrega nem ordem. As mensagens trocadas entre os peers são tuplas serializadas com `pickle`:

- `('READY', rodada, remetente)`: handshake do início da rodada.
- `('DATA', rodada, remetente, seq)`: mensagem de dados com número de sequência de 0 a `nMsgs-1`.
- `('END', rodada, remetente, nMsgs)`: o remetente não tem mais mensagens.
- `('ACK', rodada, endereço de quem confirma, seq)`: confirmação de uma das anteriores (para o `READY`, seq é -1).

Tratamento de perdas:

- Toda mensagem é enviada pela função `sendReliable`, que guarda a mensagem no dicionário `pending` até chegar o ACK do destino.
- A thread `Retransmitter` reenvia as mensagens que estão há mais de `RETRANSMIT_INTERVAL` (0,5 s) sem confirmação.
- Quem recebe sempre responde com ACK, mesmo para uma mensagem repetida, porque o ACK anterior pode ter se perdido.
- O handshake `READY` também é confirmado e reenviado. No código base ele era enviado uma vez só e o peer esperava em um laço ocupado (`while ... pass`); agora a espera usa `time.sleep`.
- Todas as mensagens levam o número da rodada, para que um reenvio atrasado de uma rodada não seja contado na seguinte.
- A thread que recebe (`MsgHandler`) fica ativa durante todo o processo, e não só durante a rodada, para continuar confirmando os reenvios dos outros peers depois que a sua rodada acabou.
- O mesmo socket UDP envia e recebe. Assim o ACK, que volta para o endereço de origem da mensagem, chega na thread que recebe.

Ordem FIFO:

- Para cada remetente, o peer guarda o próximo número de sequência esperado (`nextSeq`).
- Se a mensagem que chega é a esperada, ela é entregue (vai para o log), e em seguida são entregues as próximas que já estavam guardadas.
- Se chega uma mensagem com número maior que o esperado, ela fica guardada em `holdBack` até a anterior chegar.
- Se chega uma com número menor, é uma duplicada e é descartada.
- A rodada termina quando todos os peers mandaram `END` e as `N*nMsgs` mensagens foram entregues. As duas condições são necessárias porque um `END` pode chegar antes de um reenvio.

Perda simulada: rodando todos os peers na mesma máquina não há perda. A constante `LOSS_RATE` em `constMP.py` faz cada peer descartar ao acaso essa fração dos pacotes UDP que recebe, inclusive os ACKs. Com `LOSS_RATE = 0.2`, um em cada cinco pacotes é descartado.

Comparação dos logs: também corrigimos o laço de comparação do código base, que deixava o último peer e a maior parte das mensagens de fora (`range(1,N-1)` e `range(0,N_MSGS-1)`, quando cada log tem `N*N_MSGS` mensagens). Agora ele compara todas as posições de todos os logs e, para cada log, conta as mensagens que faltaram e as violações de FIFO.

A ordem FIFO só garante a ordem entre mensagens do mesmo remetente. Mensagens de remetentes diferentes ainda podem chegar intercaladas de forma diferente em cada peer, por isso o relatório continua mostrando rodadas fora de ordem entre os peers.

## Comparação entre sockets e RPC

Medimos o tempo da operação `list` do gerenciador de grupo com o `benchmark.py`: 100 chamadas seguidas, todos os processos em `127.0.0.1`, três execuções de cada versão (as saídas estão em `medicoes.txt`). Com sockets, a média ficou entre 0,150 e 0,164 ms por chamada. Com gRPC reaproveitando o mesmo canal, entre 0,789 e 0,844 ms. Com gRPC abrindo um canal novo a cada chamada, como a versão com sockets faz com as conexões, entre 1,323 e 1,420 ms.

Vantagens do RPC:

- Contrato explícito: as operações e os tipos estão no `mpcomm.proto`. Com sockets, o protocolo era um acordo implícito entre os arquivos (o nome das chaves do dicionário, o campo `"op"`), e uma diferença só aparecia na execução.
- Serialização e delimitação das mensagens ficam com o middleware. Com sockets, cada `recv` lia um tamanho fixo (2048 ou 32768 bytes), e um log maior que isso chegaria cortado.
- Transparência: a chamada remota é escrita como uma chamada de função, e o programa não lida com `connect`, `send` e `recv`.
- Erros tratados pelo middleware: uma operação inexistente ou um servidor fora do ar viram exceções com código (`UNIMPLEMENTED`, `UNAVAILABLE`). No código base, uma operação desconhecida era ignorada sem resposta.
- O contrato permite escrever clientes e servidores em outras linguagens, gerando o código a partir do mesmo `.proto`.

Desvantagens do RPC:

- Desempenho: nas médias das três execuções, a chamada `list` ficou cerca de 5 vezes mais lenta reaproveitando o canal (0,819 ms contra 0,156 ms) e cerca de 9 vezes abrindo um canal novo (1,371 ms). Para uma mensagem tão pequena pesa o custo do HTTP/2, da serialização com Protocol Buffers e da troca de thread no servidor. Isso não muda o resultado do sistema, porque essas chamadas acontecem só no começo e no fim de cada rodada.
- Dependência externa: é preciso instalar o `grpcio` e gerar o código de novo sempre que o `.proto` muda.
- Mais código: o `GroupMngr.py` passou de 39 para 51 linhas, por causa da classe do serviço e da criação do servidor.
- O modelo pergunta-resposta muda a estrutura dos programas. O peer, que antes esperava o sinal de início com `accept`, passou a ter um servidor gRPC, e o sinal precisa passar de uma thread para outra por uma fila.

## Como rodar

### Preparação

Requer Python 3. Na pasta do repositório:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

O código gerado (`mpcomm_pb2.py` e `mpcomm_pb2_grpc.py`) já está no repositório. Se o `mpcomm.proto` for alterado, gere de novo com:

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. mpcomm.proto
```

### Na máquina local

O `constMP.py` já vem com os endereços em `127.0.0.1` e `N = 6`. Cada comando abaixo vai em um terminal separado, com o venv ativado.

1. Gerenciador de grupo:

```bash
python GroupMngr.py
```

2. Os 6 peers, cada um com o IP, a porta UDP e a porta do serviço gRPC:

```bash
python peerCommunicatorUDP.py 127.0.0.1 6789 5681
```

```bash
python peerCommunicatorUDP.py 127.0.0.1 6790 5682
```

```bash
python peerCommunicatorUDP.py 127.0.0.1 6791 5683
```

```bash
python peerCommunicatorUDP.py 127.0.0.1 6792 5684
```

```bash
python peerCommunicatorUDP.py 127.0.0.1 6793 5685
```

```bash
python peerCommunicatorUDP.py 127.0.0.1 6794 5686
```

3. Servidor de comparação:

```bash
python comparisonServer.py
```

Ele pede o número de mensagens que cada peer deve enviar. Cada número inicia uma rodada; `0` encerra todos os processos.

Para ver o tratamento de perdas, mude `LOSS_RATE` para `0.2` no `constMP.py` antes de iniciar os peers.

### Em várias máquinas

1. Escolha uma máquina para o `GroupMngr.py` e o `comparisonServer.py` e coloque o IP dela em `SERVER_ADDR` e `GROUPMNGR_ADDR` no `constMP.py`, em todas as máquinas.
2. Ajuste `N` para o número de peers.
3. Libere as portas TCP 5678, 5679 e 5680 e a porta UDP 6789 entre as máquinas.
4. Em cada máquina de peer, rode `python peerCommunicatorUDP.py` sem argumentos. O peer usa as portas do `constMP.py` e descobre o próprio IP público em `api.ipify.org`.
5. Rode o `GroupMngr.py` antes dos peers e o `comparisonServer.py` depois deles.

Exemplo na AWS: uma instância com IP fixo (elastic IP) para os dois servidores e as instâncias dos peers divididas em duas regiões, por exemplo 6 em uma região e 2 em outra. As portas são liberadas no security group das instâncias.

### Medição

Com o `GroupMngr_sockets.py` rodando, `python benchmark.py sockets` mede a versão com sockets. Com o `GroupMngr.py` rodando, `python benchmark.py grpc` mede a versão com gRPC.

## Saída de uma execução

Seis peers em `127.0.0.1`, 20 mensagens por peer e `LOSS_RATE = 0.2`. Saída do `comparisonServer.py`:

```
Enter the number of messages for each peer to send (0 to terminate)=> List of Peers:  [('127.0.0.1', 6794, 5686), ('127.0.0.1', 6791, 5683), ('127.0.0.1', 6792, 5684), ('127.0.0.1', 6790, 5682), ('127.0.0.1', 6793, 5685), ('127.0.0.1', 6789, 5681)]
Peer process 0 started.
Peer process 1 started.
Peer process 2 started.
Peer process 3 started.
Peer process 4 started.
Peer process 5 started.
Now, wait for the message logs from the communicating peers...
Received log from peer 3
Received log from peer 4
Received log from peer 0
Received log from peer 2
Received log from peer 1
Received log from peer 5
Found 120 unordered message rounds
Log 0: 120 messages, 0 missing, 0 FIFO violations
Log 1: 120 messages, 0 missing, 0 FIFO violations
Log 2: 120 messages, 0 missing, 0 FIFO violations
Log 3: 120 messages, 0 missing, 0 FIFO violations
Log 4: 120 messages, 0 missing, 0 FIFO violations
Log 5: 120 messages, 0 missing, 0 FIFO violations
Total: 0 missing messages and 0 FIFO violations
```

Trechos da saída de um dos peers:

```
Message 1 from process 4 held back, waiting for 0
Message 3 from process 4 held back, waiting for 0
...
Received all messages. Retransmissions in this round: 75, simulated losses: 68
```

Nessa rodada, cada peer descartou entre 68 e 95 pacotes e fez entre 61 e 80 reenvios. Mesmo assim, nenhuma mensagem faltou e nenhuma chegou fora da ordem do seu remetente. Com `LOSS_RATE = 0`, a mesma rodada termina com 0 reenvios.

## Limitações

- Se um peer cair no meio de uma rodada, os outros continuam reenviando para ele e esperando as mensagens dele, e a rodada não termina.
- Os peers contam as rodadas cada um por si, então todos precisam estar registrados antes da primeira rodada.

## Integrantes

Iury Alexandre Alves Bo (202103735), Rafael Nunes Moreira Costa (202107855) e Luis Henrique Alves Rosa (202103754).
