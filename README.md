# Anti-Platform Throttling 🚀 (POC 4)

**Projeto Final de Engenharia de Sistemas Distribuídos (2026.1)**

Este repositório contém a implementação da **POC 4 - Anti-Platform Throttling**. O objetivo é simular e medir estratégias de distribuição temporal de engajamento em plataformas externas, evitando bloqueios algorítmicos.

## 👥 Equipe
* Alisson Gabriel de Campos Filho
* Antônio Rocha Lima Filho
* Cássio Vittori de Campos Filho
* João Vitor Teixeira Barreto

## 🏗️ Escopo e Arquitetura
Para garantir o envio de tráfego sem acionar defesas das plataformas, implementaremos:
1. **Fila Inteligente (Queues/PubSub):** Balanceamento de carga entre campanhas.
2. **Rate Limit / Throttling:** Controle estrito do volume de saídas.
3. **Circuit Breaker:** Pausa automática em caso de bloqueio.

## 💻 Stack Tecnológico
* **Backend:** Python
* **Banco de Dados:** PostgreSQL
* **Infraestrutura:** Docker e Docker Compose

## 🤖 Uso de Ferramentas de IA (Declaração Obrigatória)
* **Ferramentas utilizadas:** Gemini.
* **Onde foram aplicadas:** Ajuste do escopo para a POC 4, estruturação do repositório e README.
* **Orientações dadas:** Orientação para alinhar a documentação técnica com os requisitos estritos da POC 4 exigida no edital.
* **Avaliação:** Acelerou a organização e formatação, garantindo a entrega dentro do prazo estrito.
