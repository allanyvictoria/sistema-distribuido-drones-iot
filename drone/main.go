package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

func obterBrokerAddr() string {
	addr := os.Getenv("BROKER_ADDR")
	if addr == "" {
		addr = "broker:1883" // Aqui "broker" é o nome do serviço no Docker Compose
	}
	fmt.Println("Endereço do broker:", addr)
	return addr
}

func conectarBroker() (net.Conn, string, error) {
	addrPrincipal := obterBrokerAddr()

	// tenta o broker principal primeiro
	conn, err := net.DialTimeout("tcp", addrPrincipal, 5*time.Second)
	if err == nil {
		return conn, addrPrincipal, nil
	}

	// tentar os alternativos
	brokersStr := os.Getenv("BROKERS_ADDR")
	if brokersStr != "" {
		for _, broker := range strings.Split(brokersStr, ",") {
			broker = strings.TrimSpace(broker)
			if broker == "" {
				continue
			}

			addrAlternativo := broker
			if !strings.Contains(addrAlternativo, ":") {
				addrAlternativo = fmt.Sprintf("%s:1053", broker)
			}

			if addrAlternativo == addrPrincipal {
				continue
			}
			conn, err := net.DialTimeout("tcp", addrAlternativo, 5*time.Second)
			if err == nil {
				return conn, addrAlternativo, nil
			}
		}
	}
	return nil, "", fmt.Errorf("nenhum broker disponível")
}

func heartbeat(conn net.Conn, id string) {
	for {
		mensagem := fmt.Sprintf("DRONE;%s;HEARTBEAT;%s\n", id, "")
		_, err := conn.Write([]byte(mensagem))
		if err != nil {
			// Se a conexão já foi fechada, sai sem logar erro para evitar poluição
			if strings.Contains(err.Error(), "use of closed network connection") {
				return
			}
			log.Printf("Erro ao enviar heartbeat: %v", err)
			return
		}
		time.Sleep(10 * time.Second)
	}
}

func receberMissao(conn net.Conn, id string) {
	emMissao := false
	reader := bufio.NewReader(conn)
	for {
		mensagem, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("[DRONE %s]: Erro ao ler mensagem do servidor.", id)
			return
		}
		mensagem = strings.TrimSpace(mensagem)
		log.Printf("[DRONE %s] Mensagem recebida: %s", id, mensagem)

		if strings.Contains(mensagem, "MISSAO") && !emMissao {
			emMissao = true
			log.Printf("[DRONE %s] Iniciando missão!", id)
			// confirma aceite
			conn.Write([]byte(fmt.Sprintf("DRONE;%s;ACEITE;\n", id)))

			// simula missão
			time.Sleep(5 * time.Second)

			// confirma conclusão
			conn.Write([]byte(fmt.Sprintf("DRONE;%s;CONCLUSAO;\n", id)))

			emMissao = false
			log.Printf("[DRONE %s] Missão concluída!", id)
		}

	}

}

func main() {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Erro ao obter hostname: %v", err)
	}

	for {
		// Conectar ao broker

		conn, addrConectado, err := conectarBroker()
		if err != nil {
			log.Printf("Não conseguiu conectar em nenhum broker, tentando em 5s...")
			time.Sleep(5 * time.Second)
			continue
		} else {
			log.Printf("[DRONE %s] Conectado ao broker (%s) com sucesso!", hostname, addrConectado)
		}

		tipo := "DRONE"
		id := hostname
		dado := "REGISTRO"
		mensagem := fmt.Sprintf("%s;%s;%s;%s\n", tipo, id, dado, "")

		// Enviar mensagem de registro
		_, err = conn.Write([]byte(mensagem))
		if err != nil {
			log.Printf("Erro ao enviar registro: %v", err)
		}

		// Iniciar o envio de heartbeats
		go heartbeat(conn, id)

		// Iniciar a leitura de mensagens do broker
		receberMissao(conn, id)

		conn.Close()
		log.Printf("Conexão perdida, reconectando...")
	}

}
