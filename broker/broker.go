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
		payloadLimpo := strings.TrimSpace(m.Payload)

		rwmu.Lock()
		req := mapaRequisicoes[payloadLimpo]
		if req != nil {
			req.Status = "concluida"
			log.Printf("[BROKER] Conclusão remota recebida com sucesso para a requisição %s!", payloadLimpo)
			fmt.Printf("[FILA] SAIU: Req %s | Tipo: %s | Prioridade: %d | Tamanho atual: %d\n", req.ID, req.Tipo, req.Prioridade, filaRequisicoes.Len())
		} else {
			// Se cair aqui, a gente sabe que algo se perdeu!
			log.Printf("[BROKER] ALERTA: Conclusão remota recebida, mas requisição %s não encontrada localmente.", payloadLimpo)
		}
		rwmu.Unlock()
	}
}
