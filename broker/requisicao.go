package main

import (
	"container/heap"
	"fmt"
	"time"
)

const (
	PrioridadeNormal = 1
	PrioridadeMedia  = 2
	PrioridadeAlta   = 3
)

type Requisicao struct {
	ID           string
	Tipo         string
	Criticidade  string
	Prioridade   int
	Timestamp    time.Time
	Status       string
	DroneID      string
	BrokerOrigem string
}

type FilaRequisicoes []*Requisicao

// OS 5 Métodos da heap:

// Retorna o tamanho da fila
func (f FilaRequisicoes) Len() int {
	return len(f)
}

// qual tem maior prioridade
func (f FilaRequisicoes) Less(i, j int) bool {
	if f[i].Prioridade > f[j].Prioridade {
		return true
	} else if f[i].Prioridade == f[j].Prioridade {
		return f[i].Timestamp.Before(f[j].Timestamp)
	}
	return false
}

// troca dois elementos
func (f FilaRequisicoes) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

// adiciona elemento
func (f *FilaRequisicoes) Push(x any) {
	requisicao := x.(*Requisicao)
	*f = append(*f, requisicao)
	fmt.Printf("[FILA] ENTROU: Req %s | Tipo: %s | Prioridade: %d | Tamanho atual: %d\n", requisicao.ID, requisicao.Tipo, requisicao.Prioridade, len(*f))
}

// remove e retorna o de maior prioridade
func (f *FilaRequisicoes) Pop() any {
	n := len(*f)
	item := (*f)[n-1]
	*f = (*f)[:n-1]

	fmt.Printf("[FILA] SAIU: Req %s | Tipo: %s | Prioridade: %d | Tamanho atual: %d\n", item.ID, item.Tipo, item.Prioridade, len(*f))
	return item
}

func aging(r *Requisicao) {
	switch r.Prioridade {
	case 1:
		r.Prioridade = 2
	case 2:
		r.Prioridade = 3
	}
}

func iniciarAging(fila *FilaRequisicoes) {
	for {
		time.Sleep(10 * time.Second)

		rwmu.Lock()

		houveAlteracao := false

		for _, requisicao := range *fila {
			if requisicao.Status == "pendente" {
				if time.Since(requisicao.Timestamp) > 30*time.Second {
					if requisicao.Prioridade < PrioridadeAlta {
						aging(requisicao)
						houveAlteracao = true
					}
				}
			}
		}

		// Reorganiza a heap APENAS se alguma prioridade realmente subiu, poupando processamento
		if houveAlteracao {
			heap.Init(fila)
		}

		// Se sobrou alguém pendente na fila, tenta despachar de novo
		if fila.Len() > 0 {

			droneEnviado := despacharDrone()
			if houveAlteracao {
				if !droneEnviado {
					// Caiu aqui porque tentou despachar e não tinha drone
					fmt.Printf("[BROKER-1] Verificação concluída. %d requisições pendentes. Nenhum drone disponível.\n", fila.Len())
				} else {
					// Opcional: Um log de sucesso caso o drone tenha ido
					fmt.Printf("[BROKER-1] Sucesso! Drone despachado. %d requisições restantes na fila.\n", fila.Len()-1)
				} // isso funciona como um "heartbeat" de despacho.
			}
		}

		rwmu.Unlock()

	}

}
