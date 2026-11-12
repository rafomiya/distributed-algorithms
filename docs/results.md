# Resultados iniciais em processos locais

Validação exploratória da primeira implementação: 18 ensaios, três sementes por
configuração, 144 impressões medidas e 18 impressões de aquecimento. Todos os
ensaios terminaram sem colisão, duplicação de impressão ou sobreposição de seções
críticas; as réplicas convergiram ao mesmo histórico. São amostras curtas, úteis
para verificar o fluxo de coleta. A avaliação final deve repetir o procedimento
no Docker Compose e com cargas/durações maiores.

Os valores abaixo são média ± desvio padrão amostral entre três execuções.
Nas colunas de latência, cada execução contribui com sua própria média.

| Computadores | Cenário | Aquisição RA (ms) | Convergência gossip (ms) | Trabalhos/s |
|---|---|---:|---:|---:|
| 3 | Normal | 110.0 ± 11.4 | 46.6 ± 12.6 | 17.7 ± 2.2 |
| 3 | 20% perda + 10 ms | 175.5 ± 12.8 | 113.4 ± 36.1 | 11.9 ± 0.3 |
| 4 | Normal | 153.2 ± 6.4 | 75.2 ± 20.4 | 17.2 ± 1.8 |
| 4 | 20% perda + 10 ms | 224.1 ± 24.6 | 155.9 ± 46.2 | 13.2 ± 1.4 |
| 5 | Normal | 208.6 ± 14.7 | 124.4 ± 5.7 | 16.2 ± 0.4 |
| 5 | 20% perda + 10 ms | 320.3 ± 7.7 | 205.0 ± 33.0 | 11.3 ± 0.4 |

A espera para obter a impressora aumenta com o número de computadores. A carga
por computador é fixa, então o total de pedidos concorrentes também aumenta;
esses resultados não isolam o efeito do tamanho da participação do efeito da
fila. A versão atual também retransmite pedidos enquanto aguarda respostas
adiadas; o número medido de tentativas pode superar bastante `2(N-1)` por
aquisição, mesmo sem perdas. Esse é o custo da estratégia de confiabilidade,
e não uma redução da complexidade do protocolo clássico.

A perda e o atraso aumentam tanto a espera RA como a convergência do histórico.
No gossip, escolhas aleatórias dos pares e escalonamento também produzem
variabilidade; três sementes e poucos registros não sustentam estimativas
precisas de cauda. p50/p95/p99 por execução estão em `trials.csv`, e a dispersão
entre execuções está em `summary.csv`.

Configuração: duas impressoras, uma página por trabalho medido, 50 ms por página,
intervalo de gossip de 80 ms, retransmissão de 30 ms, dois trabalhos por
computador e 200 ms de aquecimento após a impressão inicial e sua convergência.
Os contadores de bytes incluem tentativas descartadas pela simulação; não são
uma medição de bytes efetivamente transmitidos na interface de rede.

Arquivos brutos e metadados: `data/experiments/initial-local/`. O backend foi
`local`, com processos HTTP independentes e sem contêineres. O Compose foi
validado sintaticamente; a execução dos contêineres não pôde ser validada neste
ambiente devido à falta de acesso ao daemon Docker.
