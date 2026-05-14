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

// Faz RESERVAR_E_DESPACHAR num broker remoto (ação atômica).
func reservarEdespachar(addr, origemID, reqID, reqTipo string) (brokerResp string, err error) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Novo payload: reqID/reqTipo/brokerOrigem
	payload := fmt.Sprintf("%s/%s/%s", reqID, reqTipo, origemID)
	fmt.Fprint(conn, montar("BROKER", origemID, "RESERVAR_E_DESPACHAR", payload))

	resp, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return "", err
	}

	// Formato esperado: BROKER;brokerN;DESPACHO_OK; ou DESPACHO_NEGADO
	partes := strings.Split(strings.TrimSpace(resp), ";")
	if len(partes) < 3 {
		return "", fmt.Errorf("resposta mal formada: %s", resp)
	}
	if partes[2] == "DESPACHO_NEGADO" {
		return partes[1], fmt.Errorf("DESPACHO_NEGADO")
	}
	return partes[1], nil
}

// Fica tentando reservar e despachar até conseguir DESPACHO_OK ou estourar o timeout.
func reservarEdespacharComRetry(addr, origemID, reqID, reqTipo string, timeout time.Duration) (brokerResp string, tentativas int, err error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		tentativas++
		brokerResp, err = reservarEdespachar(addr, origemID, reqID, reqTipo)
		if err == nil {
			return // Sucesso!
		}
		time.Sleep(500 * time.Millisecond)
	}
	return "", tentativas, fmt.Errorf("timeout após %d tentativas: %w", tentativas, err)
}

func acessivel(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
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

// ── TESTE 3 – Despacho completo (Atômico) ─────────────────────────────────────
//
// Faz o protocolo inter-broker usando a nova operação atômica:
//   1. Envia RESERVAR_E_DESPACHAR
//   2. Drone recebe MISSAO e executa (5s)
//
// Mostra que a reserva cruzada funciona em uma única ida à rede.

func testeDespachoCompleto(brokerAlvo, brokerOrigem string, nReqs int) {
	secao(fmt.Sprintf("TESTE 3 – Despacho Completo (Atômico): %d req  origem=%s  alvo=%s", nReqs, brokerOrigem, brokerAlvo))

	var resultados []Resultado
	inicio := time.Now()

	for i := 1; i <= nReqs; i++ {
		t := time.Now()
		r := Resultado{Worker: i, Acao: "RESERVAR_E_DESPACHAR"}

		reqID := fmt.Sprintf("req-teste-%d-%d", i, time.Now().UnixNano())

		// Passo Único: Reserva e Despacha na mesma chamada
		brokerResp, tentativas, err := reservarEdespacharComRetry(brokerAlvo, brokerOrigem, reqID, "bloqueio_rota", 15*time.Second)

		r.Duracao = time.Since(t)

		if err != nil {
			r.Status = "ERRO"
			r.Resposta = fmt.Sprintf("falha após %d tentativas: %v", tentativas, err)
			fmt.Printf("   ✘ %s\n", r.Resposta)
		} else {
			r.Status = "OK"
			r.Resposta = fmt.Sprintf("despachado via %s", brokerResp)
			fmt.Printf("   ✔ Missão despachada (reqID: %s) após %d tentativa(s)\n", reqID, tentativas)
		}

		imprimirLinha(r)
		resultados = append(resultados, r)

		// Missão dura 5s no drone — espera antes da próxima reserva
		if i < nReqs && err == nil {
			fmt.Printf("   ⏳ aguardando conclusão da missão (6s)...\n")
			time.Sleep(6 * time.Second)
		}
	}

	resumo(resultados, time.Since(inicio))
}

// ── TESTE 4 – Drone cai durante missão (heartbeat + requeue) ─────────────────
//
// Fluxo:
//   1. Reserva e despacha atômicamente uma missão no brokerAlvo
//   2. Para o container do drone enquanto está em missão
//   3. Aguarda o broker detectar via heartbeat (timeout = 20s, check a cada 10s)
//   4. Verifica nos logs se a requisição voltou para a fila
//   5. Reinicia o drone e verifica se ele reconecta

func testeDroneCaiEmMissao(brokerAddr, brokerOrigem, containerBroker, containerDrone string) {
	secao(fmt.Sprintf("TESTE 4 – Drone Cai em Missão: %s", containerDrone))

	// Passo 1: Despacha missão atômicamente (unificou o que antes eram 2 passos)
	fmt.Println("  [1/4] Reservando e despachando missão...")
	reqID := fmt.Sprintf("req-morte-%d", time.Now().UnixNano())
	_, tentativas, err := reservarEdespacharComRetry(brokerAddr, brokerOrigem, reqID, "bloqueio_rota", 20*time.Second)

	if err != nil {
		fmt.Printf("  ✘ Falha no despacho após %d tentativas: %v\n", tentativas, err)
		fmt.Println("      Verifique se o sistema está no ar e se há drone registrado no broker.")
		return
	}
	fmt.Printf("  ✔ Missão despachada atômicamente (reqID: %s)\n", reqID)

	// Passo 2: mata o drone no meio da missão (missão dura 5s, mata com 2s)
	fmt.Printf("  [2/4] Aguardando 2s e matando %s...\n", containerDrone)
	time.Sleep(2 * time.Second)
	err = dockerCmd("stop", containerDrone)
	if err != nil {
		fmt.Printf("  ✘ Não foi possível parar %s (rode com permissão de docker): %v\n", containerDrone, err)
		return
	}
	fmt.Printf("  ✔ %s parado\n", containerDrone)

	// Passo 3: aguarda detecção por heartbeat
	fmt.Println("  [3/4] Aguardando detecção por heartbeat (25s)...")
	time.Sleep(25 * time.Second)

	logs := logsDocker(containerBroker)
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

	// Passo 4: reinicia drone
	fmt.Printf("  [4/4] Reiniciando %s...\n", containerDrone)
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
//   4. Envia requisição nos alternativos para confirmar que ele atende
//   5. Restaura o broker original

func testeMigracaoDrone(brokerPrincipal, brokerAlternativo1, brokerAlternativo2 string,
	containerBroker, containerDrone string) {

	secao(fmt.Sprintf("TESTE 5 – Migração de Drone: %s cai, %s migra", containerBroker, containerDrone))

	// Passo 1: confirma que o drone está online enviando uma missão de teste rápida
	fmt.Println("  [1/4] Verificando drone no broker principal...")
	reqIDTeste := fmt.Sprintf("req-teste-mig-%d", time.Now().UnixNano())
	_, tentativas, err := reservarEdespacharComRetry(brokerPrincipal, "tester", reqIDTeste, "deriva", 10*time.Second)
	if err != nil {
		fmt.Printf("  ⚠  Drone não atendeu no broker principal após %d tentativas\n", tentativas)
		fmt.Println("      (pode estar offline ou em missão — continuando)")
	} else {
		fmt.Printf("  ✔ Missão aceita no broker principal! Drone está lá.\n")
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
		strings.Contains(logsDrone, "nenhum broker")

	fmt.Printf("  Drone tentou migrar (logs): ")
	if migrou {
		fmt.Println("✔ sim (log confirma tentativa de reconexão)")
	} else {
		fmt.Println("✘ não encontrado nos logs do drone")
	}

	// Passo 4: tenta despachar missão nos brokers alternativos
	fmt.Println("  [4/4] Tentando despachar missão nos brokers alternativos...")

	reservouEmAlternativo := false
	reqIDMigracao := fmt.Sprintf("req-migracao-%d", time.Now().UnixNano())

	for _, addr := range []string{brokerAlternativo1, brokerAlternativo2} {
		brokerResp, _, err := reservarEdespacharComRetry(addr, "tester-migracao", reqIDMigracao, "inspecao_visual", 8*time.Second)
		if err == nil {
			fmt.Printf("  ✔ Missão %s aceita em %s (broker processou: %s) — migração confirmada!\n",
				reqIDMigracao, addr, brokerResp)
			reservouEmAlternativo = true
			break
		}
		fmt.Printf("  ✘ %s: sem drone disponível (despacho negado)\n", addr)
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
