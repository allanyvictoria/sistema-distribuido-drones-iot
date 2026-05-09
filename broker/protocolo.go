package main

import (
	"fmt"
	"strings"
)

type Mensagem struct {
	Tipo    string
	ID      string
	Acao    string
	Payload string
}

func ParseMensagem(linha string) (Mensagem, error) {
	mensagem := strings.TrimSpace(linha)
	parts := strings.Split(mensagem, ";")

	if len(parts) < 4 {
		return Mensagem{}, fmt.Errorf("mensagem malformada: '%s'", mensagem)
	}

	return Mensagem{
		Tipo:    strings.TrimSpace(parts[0]),
		ID:      strings.TrimSpace(parts[1]),
		Acao:    strings.TrimSpace(parts[2]),
		Payload: strings.TrimSpace(parts[3]),
	}, nil
}

func ToBytes(m Mensagem) []byte {
	return []byte(fmt.Sprintf("%s;%s;%s;%s", m.Tipo, m.ID, m.Acao, m.Payload))
}
