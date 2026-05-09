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
	//log.Printf("[BROKER] Tentando despachar drone, fila: %d", filaRequisicoes.Len())
	algumDespachado := false

	for filaRequisicoes.Len() > 0 {
		// Inspeciona o topo da heap (elemento de maior prioridade)
		req := filaRequisicoes[0]

		//  se a requisição já foi concluída ou não está mais pendente, tira da fila e vai para a próxima.
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

				// remove do topo, pois alocamos o drone com sucesso
				heap.Pop(&filaRequisicoes)

				// manda mensagem pro drone
				drone.Conn.Write([]byte(fmt.Sprintf("BROKER;%s;MISSAO;%s\n", brokerID, req.Tipo)))
				droneEncontrado = true

				algumDespachado = true
				break
			}
		}

		// Se achou drone local, vai para a próxima requisição da fila
		if droneEncontrado {
			continue
		}

		//log.Printf("[BROKER] Nenhum drone local disponível, consultando outros brokers...")
		reqRemovida := heap.Pop(&filaRequisicoes)
		reqID := req.ID
		reqTipo := req.Tipo

		rwmu.Unlock()
		remotoEncontrado := false

		for _, broker := range brokers {
			//log.Printf("[BROKER] Consultando broker %s...", broker)
			conn, err := net.Dial("tcp", fmt.Sprintf("%s:1053", broker))
			if err != nil {
				//log.Printf("Erro ao conectar com broker %s: %v", broker, err)
				continue
			}

			conn.Write([]byte(fmt.Sprintf("BROKER;%s;RESERVA;\n", brokerID)))
			reader := bufio.NewReader(conn)
			linha, err := reader.ReadString('\n')

			if err != nil {
				log.Printf("Erro ao ler resposta do broker %s: %v", broker, err)
				conn.Close()
				continue
			}
			mensagem, err := ParseMensagem(linha)
			conn.Close()

			if err != nil {
				log.Printf("[BROKER] Erro ao parsear resposta do broker %s: %v | linha: %s", broker, err, linha)
				continue
			}

			if mensagem.Acao == "RESERVA_OK" {
				//log.Printf("[BROKER] Drone reservado no broker %s!", broker)

				// Manda DESPACHAR
				dispatchConn, err := net.Dial("tcp", fmt.Sprintf("%s:1053", broker))
				if err != nil {
					log.Printf("Erro ao conectar com broker %s para despachar: %v", broker, err)
					continue
				}
				dispatchConn.Write([]byte(fmt.Sprintf("BROKER;%s;DESPACHAR;%s/%s/%s\n", brokerID, mensagem.Payload, reqID, reqTipo)))
				dispatchConn.Close()

				remotoEncontrado = true
				algumDespachado = true
				break
			}
		}

		rwmu.Lock()

		if remotoEncontrado {
			// Atualiza o status
			req.Status = "em atendimento"
			continue
		} else {
			// Ninguém atendeu (nem local, nem remoto)
			heap.Push(&filaRequisicoes, reqRemovida)
			break
		}
	}
	return algumDespachado
}
