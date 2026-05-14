package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"log"
	"net"
)

// despacharDrone deve ser chamada com rwmu bloqueado
// pode liberar e readquirir o lock internamente
func despacharDrone() bool {
	algumDespachado := false

	for filaRequisicoes.Len() > 0 {
		req := filaRequisicoes[0]

		if req.Status != "pendente" {
			heap.Pop(&filaRequisicoes)
			continue
		}

		droneEncontrado := false

		// Tenta despacho local primeiro
		for _, drone := range mapaDrones {
			if drone.Disponivel {
				req.Status = "em atendimento"
				req.DroneID = drone.ID
				drone.Disponivel = false
				drone.RequisicaoAtual = req.ID

				heap.Pop(&filaRequisicoes)

				drone.Conn.Write([]byte(fmt.Sprintf("BROKER;%s;MISSAO;%s\n", brokerID, req.Tipo)))
				droneEncontrado = true
				algumDespachado = true
				break
			}
		}

		if droneEncontrado {
			continue
		}

		// Nenhum drone local — tenta brokers remotos
		reqRemovida := heap.Pop(&filaRequisicoes)
		reqID := req.ID
		reqTipo := req.Tipo

		rwmu.Unlock()
		remotoEncontrado := false

		for _, broker := range brokers {
			conn, err := net.Dial("tcp", fmt.Sprintf("%s:1053", broker))
			if err != nil {
				continue
			}

			// Operação atômica: reserva e despacha em uma única mensagem
			conn.Write([]byte(fmt.Sprintf(
				"BROKER;%s;RESERVAR_E_DESPACHAR;%s/%s/%s\n",
				brokerID, reqID, reqTipo, brokerID,
			)))

			reader := bufio.NewReader(conn)
			linha, err := reader.ReadString('\n')
			conn.Close()

			if err != nil {
				log.Printf("[BROKER] Erro ao ler resposta do broker %s: %v", broker, err)
				continue
			}

			mensagem, err := ParseMensagem(linha)
			if err != nil {
				log.Printf("[BROKER] Erro ao parsear resposta do broker %s: %v | linha: %s", broker, err, linha)
				continue
			}

			if mensagem.Acao == "DESPACHO_OK" {
				remotoEncontrado = true
				algumDespachado = true
				break
			}
		}

		rwmu.Lock()

		if remotoEncontrado {
			req.Status = "em atendimento"
			continue
		} else {
			heap.Push(&filaRequisicoes, reqRemovida)
			break
		}
	}

	return algumDespachado
}
