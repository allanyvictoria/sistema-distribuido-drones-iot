package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"log"
	"net"
	"time"
)

type Drone struct {
	ID              string
	Conn            net.Conn
	Disponivel      bool
	RequisicaoAtual string
	BrokerOrigem    string
	UltimoHeartbeat time.Time
}

func handleDrone(m Mensagem, conn net.Conn) {

	rwmu.Lock()
	novoDrone := &Drone{
		ID:              m.ID,
		Conn:            conn,
		Disponivel:      true,
		RequisicaoAtual: "",
		UltimoHeartbeat: time.Now(),
	}
	mapaDrones[m.ID] = novoDrone

	fmt.Printf("[BROKER] Novo drone conectado: %s- TOTAL: %d\n", m.ID, len(mapaDrones))
	despacharDrone()

	rwmu.Unlock()

	reader := bufio.NewReader(conn)
	for {

		linha, err := reader.ReadString('\n')
		if err != nil {
			conn.Close()
			log.Println("[SERVIDOR]: Erro ao escutar drone:", err)
			return
		}
		mensagem, err := ParseMensagem(linha)
		if err != nil {
			log.Printf("Mensagem inválida recebida: %v", err)
			return
		}

		switch mensagem.Acao {
		case "HEARTBEAT":
			rwmu.Lock()
			mapaDrones[mensagem.ID].UltimoHeartbeat = time.Now()
			rwmu.Unlock()

		case "ACEITE":
			rwmu.Lock()
			drone := mapaDrones[mensagem.ID]
			req := mapaRequisicoes[drone.RequisicaoAtual]
			if req != nil {
				req.Status = "em atendimento"
			}
			drone.Disponivel = false
			rwmu.Unlock()

		case "CONCLUSAO":
			rwmu.Lock()
			drone := mapaDrones[mensagem.ID]
			req := mapaRequisicoes[drone.RequisicaoAtual]
			brokerOrigem := ""
			reqID := ""
			if req != nil {
				req.Status = "concluida"
				brokerOrigem = drone.BrokerOrigem
				reqID = req.ID

			}
			drone.Disponivel = true
			drone.RequisicaoAtual = ""
			rwmu.Unlock()
			if brokerOrigem != "" {
				conn, err := net.Dial("tcp", fmt.Sprintf("%s:1053", brokerOrigem))
				if err != nil {
					log.Printf("Erro ao conectar com broker origem: %v", err)
				} else {
					conn.Write([]byte(fmt.Sprintf("BROKER;%s;CONCLUSAO_REMOTA;%s\n", brokerID, reqID)))
					conn.Close()
				}
			}
			rwmu.Lock()
			despacharDrone()
			rwmu.Unlock()
		}

	}
}

func verificarHeartbeat() {
	for {

		time.Sleep(10 * time.Second)
		rwmu.Lock()
		for _, drone := range mapaDrones {

			if time.Since(drone.UltimoHeartbeat) > 20*time.Second {
				log.Printf("Drone %s desconectado por timeout", drone.ID)
				drone.Conn.Close()

				if drone.RequisicaoAtual != "" {
					reqID := drone.RequisicaoAtual
					req := mapaRequisicoes[reqID]
					if req != nil {
						req.Status = "pendente"
						req.DroneID = ""
						heap.Push(&filaRequisicoes, req)
						log.Printf("[BROKER %s] Requisição %s voltou para a fila com status pendente", brokerID, req.ID)
					} else {
						// Se a requisição for de outro broker, avisa ele da conclusão remota (que na verdade é uma falha)
						log.Default().Printf("[BROKER %s] ALERTA: Missão remota %s abortada por queda do drone. Descartando da fila local.", brokerID, reqID)
					}

				}
				delete(mapaDrones, drone.ID)
			}
		}
		despacharDrone()
		rwmu.Unlock()
	}
}
