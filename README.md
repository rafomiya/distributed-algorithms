# Impressão distribuída no IC

Primeira implementação do Trabalho 2 de MC714: computadores disputam impressoras
simuladas por **Ricart–Agrawala**, e o histórico de impressões concluídas é
replicado entre os computadores por **gossip push-pull / anti-entropia**.
Os algoritmos são implementados neste repositório; aiohttp fornece apenas HTTP.

```text
Rede de computadores do IC             Rede de impressoras do IC
node1 <---- REQUEST / REPLY ----> node2
  ^                                  \
  | gossip (histórico)                 +---- printer1
  v                                  /
node3 -------------------------------+----- printer2
```

Há três computadores e duas impressoras por padrão. Cada serviço é um processo
independente. No Compose existem duas redes bridge: `computers` e `printers`.
Computadores participam de ambas; impressoras participam apenas de `printers`.
Os nomes e trabalhos simulam o IC; nenhuma impressora real é acessada.

## Executar com Docker Compose

Requisitos: Docker Engine/Desktop com acesso ao daemon, Compose v2 ou superior,
Bash, curl e Python >= 3.10. Instale a dependência Python para os clientes de
experimentos, testes e demonstração:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
scripts/sim.sh up 3 42
```

`up` inicia uma **nova execução completa**, encerrando os serviços anteriores.
São aceitos de três a cinco computadores. As duas impressoras sempre iniciam.
Os logs anteriores são preservados em diretórios distintos sob `data/logs/`.
As portas de controle são expostas apenas em localhost.

```bash
scripts/sim.sh print 1 1 2 relatorio.pdf   # computador 1, impressora 1, 2 páginas
scripts/sim.sh print 2 1 1 trabalho.pdf
scripts/sim.sh print 3 2 3 artigo.pdf
scripts/sim.sh status
curl -fs http://localhost:8002/records
scripts/sim.sh down
```

As submissões retornam HTTP 202: o trabalho permanece em espera até obter todas
as permissões. `/state` mostra `waiting_permission`, `printing` e `completed`,
as permissões faltantes, os registros conhecidos e os contadores de RPC.
A disputa é independente por impressora; impressoras diferentes podem trabalhar
em paralelo. Cada computador também mantém uma fila local por impressora.

## Demonstração e falhas

```bash
scripts/sim.sh demo
```

A demonstração verifica automaticamente: disputa normal, partição apenas do
gossip com histórico divergente e posterior convergência, partição total com
pedidos bloqueados e retomada após `heal`, e crash-stop de um computador.
Os estados de evidência são salvos em `data/demo/scenarios.json`. O ambiente
permanece ativo com `node3` desligado; encerre-o com `scripts/sim.sh down`.

Também é possível controlar as falhas manualmente:

```bash
scripts/sim.sh partition '1,2' '3'
scripts/sim.sh print 1 1
scripts/sim.sh heal
scripts/sim.sh pause printer1
scripts/sim.sh unpause printer1
scripts/sim.sh crash node3
```

`partition`/`heal` atuam no envio de mensagens da aplicação entre computadores;
as APIs administrativas continuam acessíveis. `heal` não reverte crash-stop nem
regras `tc/netem`. Suspender uma impressora mantém a seção crítica ocupada até
ela voltar e confirmar o resultado. Não existe reinício individual seguro nesta
versão: após um crash, inicie uma nova execução completa com `up`.

Para atraso/perda probabilística de RPC, com RNG separado e semente registrada:

```bash
curl -fs http://localhost:8001/fault -H 'Content-Type: application/json' \
  -d '{"delay_ms":50,"loss":0.2}'
```

`types: ["gossip"]` restringe a falha ao gossip;
`types: ["ra_request","ra_reply"]` restringe-a à coordenação.
Sem `types`, a falha afeta ambos. A perda simulada ocorre antes do envio;
respostas HTTP não são descartadas por esse mecanismo. O `netem` original também
continua disponível em `scripts/sim.sh netem 1 100 10 5` (Linux/NET_ADMIN).

## Testes e verificações

```bash
scripts/sim.sh test
python scripts/verify.py data/logs/DIRETORIO_DA_EXECUCAO
```

Os testes incluem algoritmos em uma rede simulada e integração HTTP com três
computadores e duas impressoras em **processos separados**, usando portas livres.
Não exigem Docker, mas precisam abrir sockets locais. Verificam concorrência,
fila local, independência das impressoras, perda/duplicação, respostas antigas,
idempotência de impressão, convergência, partição, crash-stop e suspensão.

O verificador de JSONL rejeita sobreposição de seções críticas, colisões no
dispositivo, impressão sem seção crítica e impressão duplicada. Intervalos ainda
abertos são reportados: são esperados se um processo cair ou ficar bloqueado.
Os ensaios normais exigem que não existam intervalos abertos ao terminar.

## Experimentos reproduzíveis

O ambiente de referência é Docker Compose:

```bash
python scripts/experiments.py --backend docker
```

Por padrão são **18 ensaios**: 3/4/5 computadores × normal/perda de 20% com 10 ms
de atraso × sementes 11/22/33. São três trabalhos por computador, divididos entre
as duas impressoras. Cada ensaio inclui impressão de aquecimento, convergência
dessa impressão e 0,5 s adicionais, excluídos da amostra. Uma rodada opcional de
partições pode ser incluída com `--scenarios normal loss partition`.

```bash
python scripts/experiments.py --backend docker --nodes 3 4 5 \
  --seeds 11 22 33 --jobs-per-node 10 --warmup 1 \
  --scenarios normal loss partition --output data/experiments/compose-final
```

Saídas: metadados de hardware/SO/Python/aiohttp/Docker/configuração em
`environment.json`, logs JSONL brutos por nó, amostras e estados por ensaio,
`trials.csv` e `summary.csv` com média, desvio padrão, mínimo e máximo entre
repetições. As latências incluem p50/p95/p99. Use um destino novo para não
misturar execuções. O backend Docker encerra o ambiente Compose existente;
execute-o quando nenhuma demonstração precisar ser preservada.

Métricas:

- **RA:** tempo desde início do processamento local até obtenção de todas as
  permissões, incluindo espera na fila local; tentativas de REQUEST/REPLY e bytes.
- **Gossip:** por registro, tempo da primeira inclusão local até inclusão na última
  réplica; tentativas de troca e bytes de aplicação.
- **Sistema:** trabalhos por segundo e duração desde a submissão da amostra até
  impressão e convergência completas. Total de trabalhos cresce com N, pois a
  carga por computador é fixa.

Bytes são JSON de aplicação, sem cabeçalhos TCP/HTTP, incluindo tentativas
simuladas como perdidas e respostas recebidas. Os contadores são separados por
algoritmo. Amostras são verificadas antes de aceitar os resultados. O verificador
e as latências entre processos pressupõem o mesmo relógio monotônico do host.

Sem Docker, os mesmos serviços podem ser exercitados em processos locais:

```bash
python scripts/scenarios.py --backend local --output data/demo-local
python scripts/experiments.py --backend local --output data/experiments/local
```

Esse backend não reproduz isolamento de redes/contêineres e seus resultados não
devem ser comparados diretamente aos do Compose. O conjunto inicial preservado
em `data/experiments/initial-local/` serve para validar o fluxo experimental;
é uma amostra curta, não uma conclusão de desempenho para o relatório.
Uma leitura inicial das métricas está em [docs/results.md](docs/results.md).

## Modelo, organização e limites

O modelo e as propriedades estão em [docs/model.md](docs/model.md). Em resumo:
participantes fixos, falhas crash-stop e partições; canais sem garantia de FIFO,
com perdas/duplicações tratadas por retransmissão e IDs. A segurança não depende
de limites temporais. A vivacidade exige participantes corretos, tarefas finitas
e comunicação que volte a permitir entrega. Timeout nunca concede permissão.

O estado dos algoritmos e a deduplicação das impressoras ficam em memória;
os JSONL são evidências e **não são relidos para recuperação**. Se um computador
cair após imprimir e antes de disseminar o registro, o histórico pode ficar
incompleto. Não se promete uma transação atômica entre impressão e replicação.
Os serviços pressupõem uma rede de laboratório confiável, sem falhas bizantinas.

```text
src/node/main.py                  computador, API, fila de trabalhos e falhas
src/node/printer.py               impressora simulada e observação de colisões
src/node/algo/ricart_agrawala.py   coordenação independente por impressora
src/node/algo/gossip.py            histórico imutável com união por job_id
scripts/cluster.py                orquestração Compose/processos e observador HTTP
scripts/experiments.py            ensaios, métricas e agregação
scripts/scenarios.py              demonstração com asserções reproduzíveis
scripts/verify.py                 verificação dos logs
```

## Entrega acadêmica

Esta versão fornece implementação, testes, Compose, demonstração e infraestrutura
experimental. Para a entrega final ainda é necessário executar/analisar ensaios
no ambiente de referência, escrever o relatório IEEE de até seis páginas sem
contar referências, informar nomes/RAs e links exigidos, gravar o vídeo com ambos
os integrantes e aplicar a tag indicada pela disciplina. O PDF original contém
a data antiga; a prorrogação informada pelo grupo deve ser considerada.

Referências dos algoritmos e origem das mudanças:

- Ricart, G.; Agrawala, A. K. (1981). [An Optimal Algorithm for Mutual Exclusion
  in Computer Networks](https://courses.grainger.illinois.edu/cs425/fa2023/358527.ricart-agrawala.pdf).
- Van Renesse et al. (2008). [Efficient Reconciliation and Flow Control for
  Anti-Entropy Protocols](https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf).
  Fundamenta a terminologia de anti-entropia; aqui foi implementada uma versão
  simples com envio do histórico completo, sem os mecanismos de otimização do artigo.
- Van Steen; Tanenbaum. *Distributed Systems*, 4ª edição, 2023.
- A base HTTP/JSON, Dockerfile e scripts de simulação vieram do próprio repositório
  e foram adaptados. Os dois algoritmos foram implementados aqui, sem biblioteca
  que forneça exclusão mútua distribuída ou replicação gossip.
