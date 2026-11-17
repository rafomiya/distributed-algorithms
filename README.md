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

Há 15 computadores e cinco impressoras por padrão. Cada serviço é um processo
independente. No Compose existem duas redes bridge: `computers` e `printers`.
Computadores participam de ambas; impressoras participam apenas de `printers`.
Os nomes e trabalhos simulam o IC; nenhuma impressora real é acessada.

## Executar com Docker Compose

Requisitos: Docker Engine/Desktop com acesso ao daemon, Compose v2 ou superior,
[just >= 1.21](https://just.systems/man/en/packages.html), Bash, curl e
Python >= 3.10. Prepare o ambiente e inicie os serviços:

```bash
just setup
just up
```

`just`, `just help` ou `just --list --unsorted` mostra os comandos, parâmetros e
descrições na ordem do arquivo. `just --color never --list --unsorted` desativa
as cores da listagem.
O justfile usa `.venv/bin/python`, sem precisar ativar o ambiente virtual.
Os argumentos de cada receita são posicionais, com padrões mostrados na ajuda.
Por exemplo, `just setup python3.12` seleciona a versão de criação do ambiente.
É possível usar outro diretório com `just venv=.venv-alt setup`, ou outro
interpretador com `just python=/caminho/python test`. As variáveis de ambiente
`VENV` e `PYTHON` também são respeitadas. Os scripts originais continuam
disponíveis em `scripts/`.

`just up` inicia uma **nova execução completa**, encerrando os serviços anteriores.
São aceitos de três a 15 computadores. As cinco impressoras sempre iniciam.
Os logs anteriores são preservados em diretórios distintos sob `data/logs/`.
As portas de controle são expostas apenas em localhost.

```bash
just print 1 1 2 relatorio.pdf          # computador, impressora, páginas, documento
just print 2 1 1 trabalho.pdf
just print 3 2 3 "artigo do IC.pdf"
just status
just records 2
just logs node1
just down
```

As submissões retornam HTTP 202: o trabalho permanece em espera até obter todas
as permissões. `/state` mostra `waiting_permission`, `printing` e `completed`,
as permissões faltantes, os registros conhecidos e os contadores de RPC.
A disputa é independente por impressora; impressoras diferentes podem trabalhar
em paralelo. Cada computador também mantém uma fila local por impressora.

O gossip mantém uma entrada versionada por computador, contendo os dados de
exemplo e todos os seus registros de impressão. Novas versões preservam os
registros anteriores; `/records` continua mostrando apenas o histórico de
impressões. As respostas de anti-entropia retornam as entradas cujas versões
o solicitante ainda não conhece.

O intervalo e o número de pares de cada rodada podem ser configurados no Compose:

```bash
GOSSIP_INTERVAL=0.25 GOSSIP_FANOUT=2 just up 3 42
```

Os padrões são 0,5 s e um par. O fanout é limitado à quantidade de pares
disponíveis; cada rodada aguarda as trocas e adiciona até 10% de jitter ao
intervalo de espera. `DATA_ITEMS` controla a quantidade de inteiros de exemplo
por entrada (padrão 5, ou 0 para desativá-los), sem alterar os registros de
impressão. Essas opções ficam visíveis em `/state`.

## Demonstração e falhas

```bash
just demo
```

A demonstração verifica automaticamente: disputa normal, partição apenas do
gossip com histórico divergente e posterior convergência, partição total com
pedidos bloqueados e retomada após `heal`, e crash-stop de um computador.
Os estados de evidência são salvos em `data/demo/scenarios.json`. O ambiente
permanece ativo com `node3` desligado; encerre-o com `just down`.

Também é possível controlar as falhas manualmente:

```bash
just partition 1,2 3
just print 1 1
just heal
just pause printer1
just unpause printer1
just crash node3
```

`partition`/`heal` atuam no envio de mensagens da aplicação entre computadores;
as APIs administrativas continuam acessíveis. `heal` não reverte crash-stop nem
regras `tc/netem`. Suspender uma impressora mantém a seção crítica ocupada até
ela voltar e confirmar o resultado. Não existe reinício individual seguro nesta
versão: após um crash, inicie uma nova execução completa com `up`.

Para atraso/perda probabilística de RPC, com RNG separado e semente registrada:

```bash
just fault 1 50 0.2
just fault 1 50 0.2 gossip
```

O quarto argumento de `just fault` seleciona os tipos de RPC: `gossip` restringe
a falha ao gossip; `"ra_request ra_reply"` restringe-a à coordenação.
Sem esse argumento, a falha afeta ambos. A perda simulada ocorre antes do envio;
respostas HTTP não são descartadas por esse mecanismo. O `netem` original também
continua disponível (Linux/NET_ADMIN):

```bash
just netem 1 100 10 5   # computador, atraso em ms, perda em %, jitter em ms
just netem-clear 1
```

A perda em `just fault` é uma fração de 0 a 1; em `just netem` é uma porcentagem
de 0 a 100.

## Testes e verificações

```bash
just test
just verify data/logs/DIRETORIO_DA_EXECUCAO
just check
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
just experiments
```

Por padrão são **18 ensaios**: 3/4/5 computadores × normal/perda de 20% com 10 ms
de atraso × sementes 11/22/33. São três trabalhos por computador, divididos entre
as duas impressoras. Cada ensaio inclui impressão de aquecimento, convergência
dessa impressão e 0,5 s adicionais, excluídos da amostra. Uma rodada opcional de
partições pode ser incluída com `--scenarios normal loss partition`.

```bash
just experiments --nodes 3 4 5 --seeds 11 22 33 \
  --jobs-per-node 10 --warmup 1 --scenarios normal loss partition \
  --output data/experiments/compose-final
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
just demo-local data/demo-local
just experiments-local --output data/experiments/local
```

As receitas experimentais encaminham os argumentos diretamente para
`scripts/experiments.py`, incluindo `--gossip-interval 0.08`,
`--partition-seconds 0.4` e `--timeout 30`. Consulte todas as opções com
`just experiments --help`. Sem `--output`, os experimentos usam uma pasta com
timestamp. Nas demonstrações, o diretório de saída é um argumento posicional
opcional, com padrão `data/demo` ou `data/demo-local`.
O runner aceita `--nodes` de 3 a 15 e `--printers` de 1 a 5; por padrão usa
duas impressoras para manter compatibilidade com a matriz experimental inicial.
`just up N SEED` configura uma execução; `--nodes` e `--seeds` definem a matriz
dos experimentos.

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
