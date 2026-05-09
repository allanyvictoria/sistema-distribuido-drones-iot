package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"log"
	"net"
	"time"
)

func definirPrioridade(criticidade string) int {
	switch criticidade {
	case "alta":
		return PrioridadeAlta
	case "media":
		return PrioridadeMedia
	case "baixa":
		return PrioridadeNormal
	default:
		return 0
	}
}

func adicionarRequisicao(m Mensagem) {
	rwmu.Lock()
	// Criar uma nova requisição com os dados do sensor

	novaRequisicao := &Requisicao{
		ID:          fmt.Sprintf("%s-%d", m.ID, time.Now().UnixNano()),
		Tipo:        m.Acao,
		Criticidade: m.Payload,
		Prioridade:  definirPrioridade(m.Payload),
		Timestamp:   time.Now(),
		Status:      "pendente",
		DroneID:     "",
	}
	mapaRequisicoes[novaRequisicao.ID] = novaRequisicao
	heap.Push(&filaRequisicoes, novaRequisicao)
	log.Printf("[BROKER] Nova requisição recebida: %s criticidade %s", novaRequisicao.Tipo, novaRequisicao.Criticidade)
	fmt.Printf("[FILA] ENTROU: Req %s | Tipo: %s | Prioridade: %d | Tamanho atual: %d\n", novaRequisicao.ID, novaRequisicao.Tipo, novaRequisicao.Prioridade, filaRequisicoes.Len())
	despacharDrone()
	rwmu.Unlock()
}

func handleSensor(m Mensagem, conn net.Conn) {

	adicionarRequisicao(m)
	reader := bufio.NewReader(conn)
	for {
		linha, err := reader.ReadString('\n')
		if err != nil {
			conn.Close()
			log.Println("[SERVIDOR]: Erro ao escutar sensor:", err)
			return
		}

		mensagem, err := ParseMensagem(linha)
		if err != nil {
			log.Printf("Mensagem inválida recebida: %v", err)
			continue
		}

		adicionarRequisicao(mensagem)
	}
}
