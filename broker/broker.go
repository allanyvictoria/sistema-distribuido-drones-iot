package main

import (
	"fmt"
	"log"
	"net"
	"strings"
)

func handleBroker(m Mensagem, conn net.Conn) {
	defer conn.Close()

	switch m.Acao {
	case "RESERVA":
		// Trava, decide, destrava
		rwmu.Lock()
		encontrou := false
		var droneID string
		for _, drone := range mapaDrones {
			if drone.Disponivel {
				drone.Disponivel = false
				droneID = drone.ID
				encontrou = true
				break
			}
		}
		rwmu.Unlock()

		if encontrou {
			conn.Write([]byte(fmt.Sprintf("BROKER;%s;RESERVA_OK;%s\n", brokerID, droneID)))
		} else {
			conn.Write([]byte(fmt.Sprintf("BROKER;%s;RESERVA_NEGADA;\n", brokerID)))
		}

	case "DESPACHAR":
		// Limpa o payload inteiro primeiro para remover \n e \r do final
		payloadLimpo := strings.TrimSpace(m.Payload)
		partes := strings.Split(payloadLimpo, "/")

		if len(partes) < 3 {
			log.Printf("[BROKER] Erro: Payload de DESPACHAR mal formatado: %s", m.Payload)
			return
		}

		// Limpa cada variável individualmente por segurança
		droneID := strings.TrimSpace(partes[0])
		reqID := strings.TrimSpace(partes[1])
		tipo := strings.TrimSpace(partes[2])

		rwmu.Lock()
		drone := mapaDrones[droneID]
		if drone != nil {
			drone.RequisicaoAtual = reqID
			drone.BrokerOrigem = m.ID // Ou m.Origem, dependendo de como está na sua struct
			drone.Conn.Write([]byte(fmt.Sprintf("BROKER;%s;MISSAO;%s\n", brokerID, tipo)))
			log.Printf("[BROKER] Drone %s despachado remotamente com sucesso!", droneID)
		} else {
			// SE CAIR AQUI: Avisa no log e (opcionalmente) destrava a reserva zumbi
			log.Printf("[BROKER] ALERTA: Tentativa de despachar drone fantasma (%s)!", droneID)
		}
		rwmu.Unlock()

	case "CONCLUSAO_REMOTA":
		rwmu.Lock()
		req := mapaRequisicoes[m.Payload]
		if req != nil {
			req.Status = "concluida"
		}
		rwmu.Unlock()
	}
}
