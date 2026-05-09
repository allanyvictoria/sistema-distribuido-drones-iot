package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ── Protocolo ─────────────────────────────────────────────────────────────────

func montar(tipo, id, acao, payload string) string {
	return fmt.Sprintf("%s;%s;%s;%s\n", tipo, id, acao, payload)
}

func enviarRequisicao(addr, id, acao, criticidade string) error {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	_, err = fmt.Fprint(conn, montar("SENSOR", id, acao, criticidade))
	return err
}

// Faz RESERVA e retorna (droneID, brokerID, err).
func reservar(addr, origemID string) (droneID string, brokerResp string, err error) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return "", "", err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprint(conn, montar("BROKER", origemID, "RESERVA", ""))
	resp, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return "", "", err
	}
	// BROKER;brokerN;RESERVA_OK;drone-id
	// BROKER;brokerN;RESERVA_NEGADA;
	partes := strings.Split(strings.TrimSpace(resp), ";")
	if len(partes) < 4 {
		return "", "", fmt.Errorf("resposta mal formada: %s", resp)
	}
	if partes[2] == "RESERVA_NEGADA" {
		return "", partes[1], fmt.Errorf("RESERVA_NEGADA")
	}
	return partes[3], partes[1], nil
}

// Faz DESPACHAR num broker remoto: droneID/reqID/tipo.
func despachar(addr, origemID, droneID, reqID, tipo string) error {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	payload := fmt.Sprintf("%s/%s/%s", droneID, reqID, tipo)
	_, err = fmt.Fprint(conn, montar("BROKER", origemID, "DESPACHAR", payload))
	return err
}

func acessivel(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// Fica tentando reserva até conseguir RESERVA_OK ou estourar o timeout.
// Retorna droneID, brokerResp e quantas tentativas fez.
func reservarComRetry(addr, origemID string, timeout time.Duration) (droneID, brokerResp string, tentativas int, err error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		tentativas++
		droneID, brokerResp, err = reservar(addr, origemID)
		if err == nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	return "", "", tentativas, fmt.Errorf("timeout após %d tentativas: %w", tentativas, err)
}

// Lê logs de um container Docker.
func logsDocker(container string) string {
	out, _ := exec.Command("docker", "logs", "--tail", "50", container).CombinedOutput()
	return string(out)
}

func dockerCmd(args ...string) error {
	return exec.Command("docker", args...).Run()
}

// ── Resultado ─────────────────────────────────────────────────────────────────

type Resultado struct {
	Worker   int
	Acao     string
	Resposta string
	Status   string
	Duracao  time.Duration
}

// ── Impressão ─────────────────────────────────────────────────────────────────

func secao(titulo string) {
	fmt.Printf("\n%s\n", strings.Repeat("═", 62))
	fmt.Printf("  %s\n", titulo)
	fmt.Printf("%s\n", strings.Repeat("═", 62))
}

func cabecalho() {
	fmt.Printf("\n%-8s %-26s %-8s %-10s %s\n", "Worker", "Ação", "Status", "Duração", "Resposta")
	fmt.Println(strings.Repeat("─", 72))
}

func imprimirLinha(r Resultado) {
	respCurta := r.Resposta
	if len(respCurta) > 28 {
		respCurta = respCurta[:25] + "..."
	}
	fmt.Printf("%-8d %-26s %-8s %-10s %s\n",
		r.Worker, r.Acao, r.Status, r.Duracao.Round(time.Millisecond), respCurta)
}

func resumo(resultados []Resultado, tempoTotal time.Duration) {
	ok, erros := 0, 0
	for _, r := range resultados {
		if r.Status == "OK" {
			ok++
		} else {
			erros++
		}
	}
	fmt.Printf("\n%s\n", strings.Repeat("─", 72))
	fmt.Printf("  Total: %d   Sucesso: %d   Erros: %d   Tempo: %s\n",
		len(resultados), ok, erros, tempoTotal.Round(time.Millisecond))
	fmt.Printf("%s\n", strings.Repeat("─", 72))
}

// ── TESTE 1 – Disponibilidade ─────────────────────────────────────────────────
//
// Verifica conectividade TCP em cada broker.
// Útil antes e depois de simular falhas.

func testeDisponibilidade(brokers []string) {
	secao("TESTE 1 – Disponibilidade dos Brokers (TCP)")
	cabecalho()

	var resultados []Resultado
	inicio := time.Now()

	for i, addr := range brokers {
		t := time.Now()
		up := acessivel(addr)
		dur := time.Since(t)

		r := Resultado{Worker: i + 1, Acao: "TCP → " + addr, Duracao: dur}
		if up {
			r.Status = "OK"
			r.Resposta = "online"
		} else {
			r.Status = "ERRO"
			r.Resposta = "sem resposta"
		}
		imprimirLinha(r)
		resultados = append(resultados, r)
	}

	resumo(resultados, time.Since(inicio))
}

// ── TESTE 2 – Concorrência ────────────────────────────────────────────────────
//
// N workers disparam requisições ao mesmo tempo para o mesmo broker.
// Todos conectam primeiro, depois disparam juntos via close(gatilho).

func testeConcorrencia(brokerAddr string, nWorkers int) {
	secao(fmt.Sprintf("TESTE 2 – Concorrência: %d workers simultâneos → %s", nWorkers, brokerAddr))

	var wg sync.WaitGroup
	ch := make(chan Resultado, nWorkers)
	gatilho := make(chan struct{})

	acoes := []string{"deriva", "bloqueio_rota", "congestionamento", "inspecao_visual"}
	crits := []string{"baixa", "media", "alta"}

	for i := 1; i <= nWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			acao := acoes[id%len(acoes)]
			crit := crits[id%len(crits)]

			conn, err := net.DialTimeout("tcp", brokerAddr, 5*time.Second)
			if err != nil {
				ch <- Resultado{id, acao + "/" + crit, err.Error(), "ERRO", 0}
				return
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(10 * time.Second))

			<-gatilho // espera sinal para disparar junto

			t := time.Now()
			_, err = fmt.Fprint(conn, montar("SENSOR", fmt.Sprintf("race-%d", id), acao, crit))
			dur := time.Since(t)

			r := Resultado{Worker: id, Acao: acao + "/" + crit, Duracao: dur}
			if err != nil {
				r.Status = "ERRO"
				r.Resposta = err.Error()
			} else {
				r.Status = "OK"
				r.Resposta = "enviado"
			}
			ch <- r
		}(i)
	}

	time.Sleep(300 * time.Millisecond)
	inicio := time.Now()
	close(gatilho)

	wg.Wait()
	close(ch)
	tempoTotal := time.Since(inicio)

	var resultados []Resultado
	for r := range ch {
		resultados = append(resultados, r)
	}

	cabecalho()
	for _, r := range resultados {
		imprimirLinha(r)
	}
	resumo(resultados, tempoTotal)
}

// ── TESTE 3 – Despacho completo (RESERVA + DESPACHAR) ────────────────────────
//
// Faz o protocolo inter-broker completo:
//   1. RESERVA → obtém droneID
//   2. DESPACHAR → drone recebe MISSAO e executa (5s)
//
// Mostra que o handshake entre brokers funciona de ponta a ponta.

func testeDespachoCompleto(brokerAlvo, brokerOrigem string, nReqs int) {
	secao(fmt.Sprintf("TESTE 3 – Despacho Completo: %d req  origem=%s  alvo=%s", nReqs, brokerOrigem, brokerAlvo))

	var resultados []Resultado
	inicio := time.Now()

	for i := 1; i <= nReqs; i++ {
		t := time.Now()
		r := Resultado{Worker: i, Acao: "RESERVA+DESPACHAR"}

		// Passo 1: RESERVA com retry (drone pode estar em missão)
		droneID, _, tentativas, err := reservarComRetry(brokerAlvo, brokerOrigem, 15*time.Second)
		if err != nil {
			r.Status = "ERRO"
			r.Resposta = fmt.Sprintf("sem drone após %d tentativas", tentativas)
			r.Duracao = time.Since(t)
			imprimirLinha(r)
			resultados = append(resultados, r)
			continue
		}

		fmt.Printf("   → drone reservado: %s (%d tentativa(s))\n", droneID, tentativas)

		// Passo 2: DESPACHAR
		reqID := fmt.Sprintf("req-teste-%d-%d", i, time.Now().UnixNano())
		err = despachar(brokerAlvo, brokerOrigem, droneID, reqID, "bloqueio_rota")
		r.Duracao = time.Since(t)

		if err != nil {
			r.Status = "ERRO"
			r.Resposta = "falha no DESPACHAR: " + err.Error()
		} else {
			r.Status = "OK"
			r.Resposta = fmt.Sprintf("drone %s em missão", droneID)
		}

		imprimirLinha(r)
		resultados = append(resultados, r)

		// Missão dura 5s no drone — espera antes da próxima reserva
		if i < nReqs {
			fmt.Printf("   ⏳ aguardando conclusão da missão (6s)...\n")
			time.Sleep(6 * time.Second)
		}
	}

	resumo(resultados, time.Since(inicio))
}

// ── TESTE 4 – Drone cai durante missão (heartbeat + requeue) ─────────────────
//
// Fluxo:
//   1. Reserva e despacha um drone em brokerAlvo
//   2. Para o container do drone enquanto está em missão
//   3. Aguarda o broker detectar via heartbeat (timeout = 20s, check a cada 10s)
//   4. Verifica nos logs se a requisição voltou para a fila (status → pendente)
//   5. Reinicia o drone e verifica se ele reconecta e a fila é atendida

func testeDroneCaiEmMissao(brokerAddr, brokerOrigem, containerBroker, containerDrone string) {
	secao(fmt.Sprintf("TESTE 4 – Drone Cai em Missão: %s", containerDrone))

	// Passo 1: garante que tem drone disponível
	fmt.Println("  [1/5] Reservando drone...")
	droneID, _, tentativas, err := reservarComRetry(brokerAddr, brokerOrigem, 20*time.Second)
	if err != nil {
		fmt.Printf("  ✘ Não foi possível reservar drone após %d tentativas: %v\n", tentativas, err)
		fmt.Println("      Verifique se o sistema está no ar e se há drone registrado no broker.")
		return
	}
	fmt.Printf("  ✔ Drone reservado: %s\n", droneID)

	// Passo 2: despacha missão
	fmt.Println("  [2/5] Despachando missão...")
	reqID := fmt.Sprintf("req-morte-%d", time.Now().UnixNano())
	err = despachar(brokerAddr, brokerOrigem, droneID, reqID, "bloqueio_rota")
	if err != nil {
		fmt.Printf("  ✘ Falha no DESPACHAR: %v\n", err)
		return
	}
	fmt.Printf("  ✔ Missão despachada (reqID: %s)\n", reqID)

	// Passo 3: mata o drone no meio da missão (missão dura 5s, mata com 2s)
	fmt.Printf("  [3/5] Aguardando 2s e matando %s...\n", containerDrone)
	time.Sleep(2 * time.Second)
	err = dockerCmd("stop", containerDrone)
	if err != nil {
		fmt.Printf("  ✘ Não foi possível parar %s (rode com permissão de docker): %v\n", containerDrone, err)
		return
	}
	fmt.Printf("  ✔ %s parado\n", containerDrone)

	// Passo 4: aguarda detecção por heartbeat
	// O broker checa a cada 10s, timeout em 20s — espera 25s pra garantir
	fmt.Println("  [4/5] Aguardando detecção por heartbeat (25s)...")
	time.Sleep(25 * time.Second)

	logs := logsDocker(containerBroker) // nome do container = host
	logsMin := strings.ToLower(logs)

	detectou := strings.Contains(logsMin, "timeout") ||
		strings.Contains(logsMin, "desconectado") ||
		strings.Contains(logsMin, "heartbeat")

	reqVoltou := strings.Contains(logsMin, "pendente") ||
		strings.Contains(logsMin, "fila") ||
		strings.Contains(logsMin, "remota") ||
		strings.Contains(logsMin, "abortada")

	fmt.Printf("  Heartbeat detectou falha: ")
	if detectou {
		fmt.Println("✔ sim")
	} else {
		fmt.Println("✘ não encontrado nos logs (verifique: docker logs " + strings.Split(brokerAddr, ":")[0] + ")")
	}

	fmt.Printf("  Requisição voltou à fila:  ")
	if reqVoltou {
		fmt.Println("✔ sim")
	} else {
		fmt.Println("✘ não encontrado nos logs")
	}

	// Passo 5: reinicia drone e verifica reconexão + atendimento da fila
	fmt.Printf("  [5/5] Reiniciando %s...\n", containerDrone)
	dockerCmd("start", containerDrone)
	time.Sleep(8 * time.Second)

	logsDrone := strings.ToLower(logsDocker(containerDrone))
	reconectou := strings.Contains(logsDrone, "registro") ||
		strings.Contains(logsDrone, "conectando") ||
		strings.Contains(logsDrone, "missao") ||
		strings.Contains(logsDrone, "missão")

	fmt.Printf("  Drone reconectou e ficou ativo: ")
	if reconectou {
		fmt.Println("✔ sim")
	} else {
		fmt.Println("✘ não encontrado nos logs do drone")
	}

	fmt.Println()
}

// ── TESTE 5 – Drone migra de setor quando o broker dele cai ──────────────────
//
// Fluxo:
//   1. Para o broker do setor do drone
//   2. Aguarda o drone perceber a queda e tentar reconectar nos brokers alternativos
//   3. Verifica nos logs do drone se ele conectou em outro broker
//   4. Tenta reservar o drone no novo broker (RESERVA_OK = migração confirmada)
//   5. Restaura o broker original

func testeMigracaoDrone(brokerPrincipal, brokerAlternativo1, brokerAlternativo2 string,
	containerBroker, containerDrone string) {

	secao(fmt.Sprintf("TESTE 5 – Migração de Drone: %s cai, %s migra", containerBroker, containerDrone))

	// Passo 1: confirma que o drone está registrado no broker principal
	fmt.Println("  [1/4] Verificando drone no broker principal...")
	droneID, _, tentativas, err := reservarComRetry(brokerPrincipal, "tester", 10*time.Second)
	if err != nil {
		fmt.Printf("  ⚠  Drone não disponível no broker principal após %d tentativas\n", tentativas)
		fmt.Println("      (pode estar em missão — tudo bem, continuando)")
	} else {
		// Libera o drone reservado mandando um despacho fictício não vai funcionar,
		// então só loga que estava disponível
		fmt.Printf("  ✔ Drone %s estava disponível no broker principal\n", droneID)
		// Devolvemos o drone reservando e não despachando — ele ficará "indisponível"
		// mas o heartbeat vai detectar a queda do broker e limpar
	}

	// Passo 2: para o broker principal
	fmt.Printf("  [2/4] Parando %s...\n", containerBroker)
	err = dockerCmd("stop", containerBroker)
	if err != nil {
		fmt.Printf("  ✘ Não foi possível parar %s: %v\n", containerBroker, err)
		return
	}
	fmt.Printf("  ✔ %s parado\n", containerBroker)

	// Passo 3: aguarda drone reconectar (o drone tenta a cada 5s)
	fmt.Println("  [3/4] Aguardando drone reconectar em broker alternativo (15s)...")
	time.Sleep(15 * time.Second)

	logsDrone := strings.ToLower(logsDocker(containerDrone))
	migrou := strings.Contains(logsDrone, "reconect") ||
		strings.Contains(logsDrone, brokerAlternativo1[:strings.Index(brokerAlternativo1, ":")]) ||
		strings.Contains(logsDrone, brokerAlternativo2[:strings.Index(brokerAlternativo2, ":")]) ||
		strings.Contains(logsDrone, "nenhum broker") // tentou, mesmo que falhou

	fmt.Printf("  Drone tentou migrar: ")
	if migrou {
		fmt.Println("✔ sim (log confirma tentativa de reconexão)")
	} else {
		fmt.Println("✘ não encontrado nos logs do drone")
	}

	// Passo 4: tenta reservar o drone nos brokers alternativos
	fmt.Println("  [4/4] Tentando reservar drone nos brokers alternativos...")

	reservouEmAlternativo := false
	for _, addr := range []string{brokerAlternativo1, brokerAlternativo2} {
		droneID, broker, _, err := reservarComRetry(addr, "tester-migracao", 8*time.Second)
		if err == nil {
			fmt.Printf("  ✔ Drone %s encontrado em %s (broker: %s) — migração confirmada!\n",
				droneID, addr, broker)
			reservouEmAlternativo = true
			break
		}
		fmt.Printf("  ✘ %s: sem drone disponível\n", addr)
	}

	if !reservouEmAlternativo {
		fmt.Println("  ⚠  Drone ainda não migrou ou está em missão.")
		fmt.Printf("     Verifique: docker logs %s\n", containerDrone)
	}

	// Restaura broker principal
	fmt.Printf("\n  Restaurando %s...\n", containerBroker)
	dockerCmd("start", containerBroker)
	time.Sleep(5 * time.Second)
	fmt.Printf("  ✔ %s restaurado\n", containerBroker)
	fmt.Println()
}

// ── TESTE 6 – Missão remota: drone de outro setor atende e requisição fecha ───
//
// Fluxo:
//   1. Para o drone local do brokerAlvo (sem drone local disponível)
//   2. Envia requisição ao brokerAlvo
//   3. brokerAlvo deve consultar outros brokers e despachar drone remoto
//   4. Verifica nos logs se aparece despacho remoto e conclusão

func testeMissaoRemota(brokerAlvo, containerDroneLocal string, brokerAddr string) {
	secao(fmt.Sprintf("TESTE 6 – Missão Remota: %s sem drone local", brokerAddr))

	// Para o drone local
	fmt.Printf("  [1/3] Parando drone local (%s)...\n", containerDroneLocal)
	err := dockerCmd("stop", containerDroneLocal)
	if err != nil {
		fmt.Printf("  ✘ Não foi possível parar %s: %v\n", containerDroneLocal, err)
		return
	}
	fmt.Printf("  ✔ %s parado\n", containerDroneLocal)
	time.Sleep(3 * time.Second)

	// Envia requisição ao broker sem drone local
	fmt.Println("  [2/3] Enviando requisição ao broker (sem drone local)...")
	err = enviarRequisicao(brokerAddr, "sensor-remoto-test", "bloqueio_rota", "alta")
	if err != nil {
		fmt.Printf("  ✘ Erro ao enviar requisição: %v\n", err)
	} else {
		fmt.Println("  ✔ Requisição enviada")
	}

	// Aguarda o broker consultar remotamente e o drone remoto executar
	fmt.Println("  [3/3] Aguardando despacho remoto (10s)...")
	time.Sleep(10 * time.Second)

	// Verifica logs do broker alvo
	nomeContainer := brokerAlvo
	logs := strings.ToLower(logsDocker(nomeContainer))
	despacheuRemoto := strings.Contains(logs, "reserva") ||
		strings.Contains(logs, "remoto") ||
		strings.Contains(logs, "despach")

	fmt.Printf("  Broker consultou drone remoto: ")
	if despacheuRemoto {
		fmt.Println("✔ sim")
	} else {
		fmt.Println("✘ não encontrado nos logs")
	}

	// Restaura drone local
	fmt.Printf("\n  Restaurando %s...\n", containerDroneLocal)
	dockerCmd("start", containerDroneLocal)
	time.Sleep(5 * time.Second)
	fmt.Printf("  ✔ %s restaurado\n", containerDroneLocal)
	fmt.Println()
}

// ── Main ──────────────────────────────────────────────────────────────────────

func uso() {
	fmt.Println(`
Uso: go run teste.go <teste> [opções]

Testes disponíveis:

  disponivel   <broker1:porta> [broker2:porta ...]
               Verifica conectividade TCP em cada broker.
               Ex: go run teste.go disponivel localhost:1053 localhost:1054 localhost:1055

  concorrencia <broker:porta> <n_workers>
               N workers disparam requisições ao mesmo tempo.
               Ex: go run teste.go concorrencia localhost:1053 20

  despacho     <broker_alvo:porta> <broker_origem_id> <n_requisicoes>
               Faz RESERVA + DESPACHAR completo (protocolo inter-broker real).
               Ex: go run teste.go despacho localhost:1054 broker1 3

  drone_cai    <broker:porta> <broker_origem_id> <container_drone>
               Reserva e despacha um drone, mata o container no meio da missão
               e verifica se o broker detecta via heartbeat e volta à fila.
               Ex: go run teste.go drone_cai localhost:1053 broker2 drone-setor1

  migracao     <broker_principal:porta> <broker_alt1:porta> <broker_alt2:porta> <container_broker> <container_drone>
               Para o broker do setor e verifica se o drone migra para outro.
               Ex: go run teste.go migracao localhost:1053 localhost:1054 localhost:1055 broker1 drone-setor1

  missao_remota <container_broker_alvo> <container_drone_local> <broker_alvo:porta>
               Para o drone local de um setor e verifica se o broker busca drone remoto.
               Ex: go run teste.go missao_remota broker1 drone-setor1 localhost:1053
`)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		uso()
	}

	switch os.Args[1] {

	case "disponivel":
		if len(os.Args) < 3 {
			uso()
		}
		testeDisponibilidade(os.Args[2:])

	case "concorrencia":
		if len(os.Args) < 4 {
			uso()
		}
		n, err := strconv.Atoi(os.Args[3])
		if err != nil || n < 1 {
			fmt.Println("n_workers deve ser inteiro positivo")
			os.Exit(1)
		}
		testeConcorrencia(os.Args[2], n)

	case "despacho":
		if len(os.Args) < 5 {
			uso()
		}
		n, err := strconv.Atoi(os.Args[4])
		if err != nil || n < 1 {
			fmt.Println("n_requisicoes deve ser inteiro positivo")
			os.Exit(1)
		}
		testeDespachoCompleto(os.Args[2], os.Args[3], n)

	case "drone_cai":
		if len(os.Args) < 6 {
			uso()
		}
		testeDroneCaiEmMissao(os.Args[2], os.Args[3], os.Args[4], os.Args[5])

	case "migracao":
		if len(os.Args) < 7 {
			uso()
		}
		testeMigracaoDrone(os.Args[2], os.Args[3], os.Args[4], os.Args[5], os.Args[6])

	case "missao_remota":
		if len(os.Args) < 5 {
			uso()
		}
		testeMissaoRemota(os.Args[2], os.Args[3], os.Args[4])

	default:
		uso()
	}
}
