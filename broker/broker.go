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
	case "RESERVAR_E_DESPACHAR":
		partes := strings.Split(strings.TrimSpace(m.Payload), "/")
		reqID, reqTipo, brokerOrigem := partes[0], partes[1], partes[2]

		rwmu.Lock()
		for _, drone := range mapaDrones {
			if drone.Disponivel {
				drone.Disponivel = false
				drone.RequisicaoAtual = reqID
				drone.BrokerOrigem = brokerOrigem
				drone.Conn.Write([]byte(fmt.Sprintf(
					"BROKER;%s;MISSAO;%s\n", brokerID, reqTipo,
				)))
				rwmu.Unlock()
				conn.Write([]byte(fmt.Sprintf(
					"BROKER;%s;DESPACHO_OK;\n", brokerID,
				)))
				return
			}
		}
		rwmu.Unlock()
		conn.Write([]byte(fmt.Sprintf("BROKER;%s;DESPACHO_NEGADO;\n", brokerID)))

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
