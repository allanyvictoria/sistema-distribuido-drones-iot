# Estreito de Ormuz — Infraestrutura Distribuída de Drones

Sistema distribuído para coordenação de uma frota de drones autônomos de monitoramento marítimo, desenvolvido em Go e containerizado com Docker. Brokers independentes gerenciam setores do estreito, compartilham uma frota de drones e continuam operando mesmo com falhas parciais, sem nenhum ponto central de controle.

---

## Estrutura de Diretórios

```
.
├── docker-compose.yml
├── Teste.go                        # Suite de testes TCP externos
├── broker/
│   ├── Dockerfile
│   ├── main.go                     # Inicialização do servidor TCP, goroutines de heartbeat e aging
│   ├── broker.go                   # Protocolo inter-broker: RESERVA, DESPACHAR, CONCLUSAO_REMOTA
│   ├── despacho.go                 # Lógica de despacho local e remoto, heap de prioridade
│   ├── drone.go                    # Registro de drones, heartbeat, verificação de timeout
│   ├── sensor.go                   # Recepção de requisições dos sensores, inserção na fila
│   ├── requisicao.go               # Struct Requisicao, FilaRequisicoes (heap), aging de prioridade
│   ├── protocolo.go                # Struct Mensagem, ParseMensagem, ToBytes
│   └── go.mod
├── drone/
│   ├── Dockerfile
│   ├── main.go                     # Conecta ao broker, envia heartbeat, executa missões
│   └── go.mod
├── sensor/
│   ├── Dockerfile
│   ├── main.go                     # Gera requisições aleatórias autonomamente
│   └── go.mod
└── sensor-manual/
    ├── Dockerfile
    ├── main.go                     # Interface de terminal para injetar requisições manualmente
    └── go.mod
```

---

## Pacotes e Dependências

O projeto utiliza **apenas a biblioteca padrão do Go**, sem frameworks externos:

| Pacote | Uso |
|--------|-----|
| `net` | Sockets TCP (brokers, drones, sensores) |
| `sync` | `Mutex` para proteção de mapas e fila compartilhados |
| `container/heap` | Fila de prioridade das requisições |
| `bufio` | Leitura de mensagens linha a linha via TCP |
| `time` | Timestamps, heartbeat, aging, timeout de conexão |
| `fmt` / `log` | Saída e logging |
| `math/rand` | Geração aleatória de tipos e criticidades no sensor |
| `strings` | Parsing de mensagens e variáveis de ambiente |
| `strconv` | Conversão do intervalo de envio do sensor |
| `os` | Hostname (ID do container), variáveis de ambiente |

---

## Protocolo de Comunicação

Todas as mensagens seguem o formato:

```
TIPO;ID;ACAO;PAYLOAD\n
```

| Campo | Descrição |
|-------|-----------|
| `TIPO` | Origem da mensagem: `SENSOR`, `DRONE`, `BROKER` |
| `ID` | Identificador do remetente (hostname do container) |
| `ACAO` | Ação ou estado: `REGISTRO`, `MISSAO`, `HEARTBEAT`, `RESERVA`, etc. |
| `PAYLOAD` | Dado adicional (criticidade, droneID, reqID, etc.) |

### Mensagens por componente

**Sensor → Broker** (requisição de monitoramento):
```
SENSOR;sensor-setor1-deriva;bloqueio_rota;alta
```

**Drone → Broker** (registro ao conectar):
```
DRONE;drone-setor1;REGISTRO;
```

**Drone → Broker** (heartbeat periódico, a cada 10s):
```
DRONE;drone-setor1;HEARTBEAT;
```

**Drone → Broker** (aceite e conclusão de missão):
```
DRONE;drone-setor1;ACEITE;
DRONE;drone-setor1;CONCLUSAO;
```

**Broker → Drone** (despacho de missão):
```
BROKER;broker1;MISSAO;bloqueio_rota
```

**Broker → Broker** (reserva de drone remoto):
```
BROKER;broker1;RESERVA;
→ BROKER;broker2;RESERVA_OK;drone-setor2
→ BROKER;broker2;RESERVA_NEGADA;
```

**Broker → Broker** (despacho remoto):
```
BROKER;broker1;DESPACHAR;drone-setor2/req-xyz/bloqueio_rota
```

**Broker → Broker** (conclusão de missão remota):
```
BROKER;broker2;CONCLUSAO_REMOTA;req-xyz
```

---

## Como Executar

### Pré-requisitos

- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)
- [Go 1.22+](https://golang.org/) (apenas para rodar os testes)

### Subindo o ambiente completo

```bash
# Constrói as imagens e sobe brokers, drones e sensores automáticos
docker compose up --build
```

### Sensor manual (terminal interativo)

Para injetar requisições manualmente, em um terminal separado:

```bash
docker compose --profile manual run --rm sensor-manual
```

O menu guia a escolha do setor, tipo de ocorrência e criticidade antes de enviar.

### Derrubando o ambiente

```bash
docker compose down -v
```

### Executando em máquinas distintas (laboratório)

No laboratório, cada serviço pode rodar em uma máquina diferente. Ajuste as variáveis de ambiente no `docker-compose.yml` para apontar para os IPs reais das máquinas:

```yaml
environment:
  - BROKER_ADDR=192.168.1.10:1053
  - BROKERS_ADDR=192.168.1.11,192.168.1.12
```

Os brokers se comunicam entre si pelo nome do container dentro da rede `estreito-net` (quando no mesmo host) ou pelo IP da máquina (quando em hosts distintos).

---

## Como Usar

### Sensor automático

Cada sensor gera requisições aleatórias a cada `INTERVALO` segundos (padrão: 5s) e as envia ao broker do seu setor. O tipo de ocorrência e a criticidade são sorteados a cada envio:

```
[SENSOR bloqueio_rota] Criticidade: alta | Horário: 2026-05-09 14:32:11
```

### Sensor manual

Ao iniciar, exibe um menu interativo:

```
=== SETOR ===
  [1] Setor 1 (broker1)
  [2] Setor 2 (broker2)
  [3] Setor 3 (broker3)

=== TIPO DE SENSOR ===
  [1] bloqueio_rota
  [2] deriva
  ...

=== CRITICIDADE ===
  [1] baixa
  [2] media
  [3] alta
```

### Drone

O drone conecta ao broker do seu setor, registra-se e aguarda missões. Ao receber `MISSAO`, confirma com `ACEITE`, simula a execução (5s) e responde com `CONCLUSAO`:

```
[DRONE drone-setor1] Conectado ao broker (broker1:1053) com sucesso!
[DRONE drone-setor1] Mensagem recebida: BROKER;broker1;MISSAO;bloqueio_rota
[DRONE drone-setor1] Iniciando missão!
[DRONE drone-setor1] Missão concluída!
```

Se o broker cair, o drone tenta reconectar automaticamente nos brokers alternativos (definidos em `BROKERS_ADDR`) a cada 5s.

### Broker

Exibe no terminal as requisições recebidas, o estado da fila e os despachos:

```
[BROKER broker1]: Servidor iniciado na porta 1053
[BROKER] Nova requisição recebida: bloqueio_rota criticidade alta
[FILA] ENTROU: Req sensor-setor1-1234 | Tipo: bloqueio_rota | Prioridade: 3 | Tamanho atual: 1
[BROKER] Drone drone-setor1 despachado remotamente com sucesso!
[BROKER-1] Sucesso! Drone despachado. 0 requisições restantes na fila.
```

---

## Arquitetura

```
        SETOR 1                  SETOR 2                  SETOR 3
  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
  │  sensor-deriva   │    │  sensor-objeto   │    │ sensor-inspecao  │
  │  sensor-bloqueio │    │  sensor-congest. │    │ sensor-risco     │
  └────────┬─────────┘    └────────┬─────────┘    └────────┬─────────┘
           │ TCP                   │ TCP                    │ TCP
           ▼                       ▼                        ▼
  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
  │    broker1      │◀──▶│    broker2      │◀──▶│    broker3      │
  │  :1053 / 1053   │    │  :1054 / 1053   │    │  :1055 / 1053   │
  └────────▲────────┘    └────────▲────────┘    └────────▲────────┘
           │ TCP                   │ TCP                    │ TCP
  ┌────────┴────────┐    ┌────────┴────────┐    ┌────────┴────────┐
  │  drone-setor1   │    │  drone-setor2   │    │  drone-setor3   │
  └─────────────────┘    └─────────────────┘    └─────────────────┘

  Frota compartilhada: qualquer broker pode requisitar drone de outro setor
```

Cada broker gerencia seu setor de forma autônoma. Quando não há drone local disponível, o broker consulta os demais via protocolo `RESERVA` / `DESPACHAR`. A conclusão de missões remotas é notificada de volta ao broker de origem via `CONCLUSAO_REMOTA`.

---

## Fila de Prioridade e Aging

As requisições entram numa `container/heap` ordenada por prioridade e, dentro da mesma prioridade, por timestamp (FIFO):

| Criticidade | Prioridade |
|-------------|------------|
| `alta` | 3 |
| `media` | 2 |
| `baixa` | 1 |

O **aging** evita que requisições de baixa prioridade fiquem esperando indefinidamente. A cada 10s, requisições pendentes há mais de 30s têm a prioridade elevada em um nível. Quando há alteração, a heap é reorganizada e o broker tenta despachar novamente.

---

## Concorrência e Tolerância a Falhas

- `sync.Mutex` protege `mapaDrones`, `mapaRequisicoes` e `filaRequisicoes` contra acesso concorrente
- Cada conexão TCP (sensor, drone, broker remoto) roda em goroutine dedicada
- O lock é liberado antes de chamadas de rede inter-broker (para não bloquear o sistema inteiro durante uma consulta remota) e readquirido logo após
- **Heartbeat:** o drone envia `HEARTBEAT` a cada 10s; o broker verifica a cada 10s e remove drones sem heartbeat há mais de 20s
- **Requeue:** se um drone cai durante uma missão, a requisição volta à fila com status `pendente` e é redespachada automaticamente
- **Sem SPOF:** se um broker cair, os demais continuam operando independentemente; drones tentam reconectar nos brokers alternativos listados em `BROKERS_ADDR`

---

## Testes

O arquivo `Teste.go` permite testar o sistema externamente via TCP, simulando cenários reais de uso e falhas na rede, sem a necessidade de acessar os containers manualmente.

### Testes Básicos e de Concorrência
Valida o comportamento do sistema sob carga e a capacidade de processamento básico da fila de prioridades.

```bash
# Verifica quais brokers da rede estão online respondendo a requisições
go run Teste.go disponivel localhost:1053 localhost:1054 localhost:1055

# Envia 20 requisições sequenciais para validar a aceitação e o fluxo padrão
go run Teste.go sanidade localhost:1053 20

# Simula 50 sensores disparando alertas simultaneamente (Teste de Carga)
go run Teste.go concorrencia localhost:1053 50

# Dispara um fluxo intenso mesclando missões de criticidade Alta, Média e Baixa
go run Teste.go flood localhost:1053 20

```

### Testes de Arquitetura Distribuída

Avalia a comunicação entre múltiplos Brokers utilizando os protocolos TCP definidos.

```bash
# Testa o protocolo inter-broker completo (RESERVA -> ACEITE -> DESPACHO)
go run Teste.go despacho localhost:1054 broker1 2

# Esgota/para os drones locais para forçar o broker a buscar recursos em um setor vizinho
go run Teste.go missao_remota broker1 drone-setor1 localhost:1053

```

### Testes de Resiliência (Tolerância a Falhas)

Cenários de caos para garantir que o sistema se recupera de problemas de infraestrutura de forma autônoma.

```bash
# Simula a queda de um drone durante uma missão: valida o Heartbeat, Timeout e Re-enfileiramento
go run Teste.go drone_cai localhost:1053 broker2 drone-setor1

# Derruba o broker de um setor inteiro e verifica se o drone migra para outro broker sobrevivente
go run Teste.go migracao localhost:1053 localhost:1054 localhost:1055 broker1 drone-setor1

```

> ** Requisito para os testes de falha:**
> Os cenários `drone_cai` e `migracao` executam comandos como `docker stop` / `docker start` internamente para simular as quedas físicas. É necessário rodar esses testes específicos em um terminal que possua permissões ativas para interagir com o daemon do Docker.

```

```