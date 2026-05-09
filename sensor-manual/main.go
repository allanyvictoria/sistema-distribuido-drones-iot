package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

// Brokers de cada setor (ajuste se necessário)
var setores = map[string]string{
	"1": "broker1:1053",
	"2": "broker2:1053",
	"3": "broker3:1053",
}

var tiposSensor = []string{
	"bloqueio_rota",
	"deriva",
	"congestionamento",
	"objeto_nao_identificado",
	"inspecao_visual",
	"risco_ambiental",
}

var criticidades = []string{
	"baixa",
	"media",
	"alta",
}

func escolher(titulo string, opcoes []string) string {
	for {
		fmt.Printf("\n=== %s ===\n", titulo)
		for i, op := range opcoes {
			fmt.Printf("  [%d] %s\n", i+1, op)
		}
		fmt.Print("Escolha: ")
		reader := bufio.NewReader(os.Stdin)
		linha, _ := reader.ReadString('\n')
		linha = strings.TrimSpace(linha)
		n, err := strconv.Atoi(linha)
		if err == nil && n >= 1 && n <= len(opcoes) {
			return opcoes[n-1]
		}
		fmt.Println("Opção inválida, tente novamente.")
	}
}

func escolherSetor() (string, string) {
	setoresOpts := []string{"Setor 1 (broker1)", "Setor 2 (broker2)", "Setor 3 (broker3)"}
	escolha := escolher("SETOR", setoresOpts)
	num := strings.Split(escolha, " ")[1] // "1", "2" ou "3"
	return num, setores[num]
}

func enviar(brokerAddr, tipoSensor, criticidade string) {
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		fmt.Printf("[ERRO] Não foi possível conectar em %s: %v\n", brokerAddr, err)
		return
	}
	defer conn.Close()

	msg := fmt.Sprintf("SENSOR;%s;DADO;%s\n", tipoSensor, criticidade)
	_, err = conn.Write([]byte(msg))
	if err != nil {
		fmt.Printf("[ERRO] Falha ao enviar: %v\n", err)
		return
	}
	fmt.Printf("\n✔ Enviado para %s → tipo: %s | criticidade: %s\n", brokerAddr, tipoSensor, criticidade)
}

func main() {
	fmt.Println("╔══════════════════════════════════════╗")
	fmt.Println("║     SENSOR MANUAL - MODO CLIENTE     ║")
	fmt.Println("╚══════════════════════════════════════╝")

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("\n----------------------------------------")

		// Escolhe setor
		setorNum, brokerAddr := escolherSetor()
		fmt.Printf("→ Setor %s | Broker: %s\n", setorNum, brokerAddr)

		// Escolhe tipo de sensor
		tipo := escolher("TIPO DE SENSOR", tiposSensor)

		// Escolhe criticidade
		crit := escolher("CRITICIDADE", criticidades)

		// Confirmação
		fmt.Printf("\nConfirmar envio?\n  Setor %s | %s | criticidade: %s\n  [s] Sim  [n] Cancelar\n> ", setorNum, tipo, crit)
		resp, _ := reader.ReadString('\n')
		resp = strings.TrimSpace(strings.ToLower(resp))

		if resp == "s" || resp == "sim" {
			enviar(brokerAddr, tipo, crit)
		} else {
			fmt.Println("Cancelado.")
		}

		// Continuar?
		fmt.Print("\nEnviar outro? [s/n]: ")
		cont, _ := reader.ReadString('\n')
		cont = strings.TrimSpace(strings.ToLower(cont))
		if cont != "s" && cont != "sim" {
			fmt.Println("Encerrando sensor manual.")
			break
		}
	}
}
