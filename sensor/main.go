package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"
)

func obterIntervalo() int {
	intervaloStr := os.Getenv("INTERVALO")
	if intervaloStr == "" {
		return 5
	}

	// Convertendo o valor da variável de ambiente para int
	intervalo, err := strconv.Atoi(intervaloStr)
	if err != nil {
		// Se a conversão falhar, retorna o valor padrão
		log.Printf("Erro ao obter intervalo:, %s", err)
		return 5
	}

	return intervalo
}

func obterBrokerAddr() string {
	addr := os.Getenv("BROKER_ADDR")
	if addr == "" {
		addr = "broker:1053" // Aqui "server" é o nome do serviço no Docker Compose
	}
	fmt.Println("Endereço do broker:", addr)
	return addr
}

func conectarBroker() (net.Conn, error) {
	conn, err := net.Dial("tcp", obterBrokerAddr())
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func main() {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Erro ao obter o hostname: %v", err)
	}

	// Tentativa de conexão com o broker
	conn, err := conectarBroker()
	if err != nil {
		log.Fatalf("Erro inicial ao conectar ao servidor: %v", err)
	}
	defer conn.Close()

	for {
		// Tipos de sensores e criticidades
		tipos := []string{
			"deriva",
			"bloqueio_rota",
			"objeto_nao_identificado",
			"congestionamento",
			"inspecao_visual",
			"risco_ambiental",
		}
		criticidades := []string{"baixa", "media", "alta"}
		tipoSensor := tipos[rand.Intn(len(tipos))]
		nivelcriticidade := criticidades[rand.Intn(len(criticidades))]

		tipo := "SENSOR"
		id := hostname
		dado := tipoSensor
		criticidade := nivelcriticidade

		mensagem := fmt.Sprintf("%s;%s;%s;%s\n", tipo, id, dado, criticidade)

		_, err := conn.Write([]byte(mensagem))
		if err != nil {
			log.Printf("Erro ao enviar mensagem: %v. Tentando reconectar...", err)
			// Tenta reconectar
			// Fecha a conexão antiga para evitar vazamento de recursos
			if conn != nil {
				conn.Close()
			}

			for {
				conn, err = conectarBroker()
				if err == nil {
					log.Println("Reconectado com sucesso ao servidor.")
					break // Conseguiu conectar! Sai do laço de reconexão.
				}

				log.Printf("Erro ao reconectar: %v. Tentando novamente em breve...", err)
				time.Sleep(5 * time.Second) // Espera antes de tentar novamente
			}
		}

		// Interface terminal do sensor
		fmt.Printf("\r\033[2k\r[SENSOR %s] Criticidade: %s | Horário: %s", dado, criticidade, time.Now().UTC().Format("2006-01-02 15:04:05"))
		log.Printf("[SENSOR %s] Enviando dado: %s", dado, criticidade)
		time.Sleep(time.Duration(obterIntervalo()) * time.Second)
	}

}
