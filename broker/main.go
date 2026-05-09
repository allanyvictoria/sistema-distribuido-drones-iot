package main

import (
	"bufio"
	"container/heap"
	"log"
	"net"
	"os"
	"strings"
	"sync"
)

var rwmu sync.Mutex
var mapaDrones = make(map[string]*Drone)
var mapaRequisicoes = make(map[string]*Requisicao)
var filaRequisicoes FilaRequisicoes
var brokers []string
var brokerID string

func handleConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)
	linha, err := reader.ReadString('\n')
	if err != nil {
		conn.Close()
		log.Printf("Erro ao ler mensagem inicial: %v", err)
		return
	}
	mensagem, err := ParseMensagem(linha)
	if err != nil {
		conn.Close()
		log.Printf("Mensagem inicial inválida: %v", err)
		return
	}
	switch mensagem.Tipo {
	case "SENSOR":
		handleSensor(mensagem, conn)
	case "DRONE":
		handleDrone(mensagem, conn)
	case "BROKER":
		handleBroker(mensagem, conn)
	}
}

func main() {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Erro ao obter hostname: %v", err)
	}
	brokerID = hostname

	brokersStr := os.Getenv("BROKERS_ADDR")
	if brokersStr != "" {
		brokers = strings.Split(brokersStr, ",")
	}

	heap.Init(&filaRequisicoes)

	log.Printf("[BROKER %s]: Iniciando broker...", brokerID)

	// inicia goroutine de heartbeat
	go verificarHeartbeat()

	// inicia goroutine de aging
	go iniciarAging(&filaRequisicoes)

	// inicia servidor para drones
	ln, err := net.Listen("tcp", ":1053")
	if err != nil {
		log.Fatalf("Erro ao iniciar servidor: %v", err)
	}
	defer ln.Close()
	log.Printf("[BROKER %s]: Servidor iniciado na porta 1053", brokerID)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Erro ao aceitar conexão: %v", err)
			continue
		}
		go handleConnection(conn)
	}

}
