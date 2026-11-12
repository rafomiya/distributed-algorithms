# Modelo do sistema e propriedades

## Participantes e aplicação

Há N computadores conhecidos, com IDs únicos `node1..nodeN`, e duas impressoras
`printer1` e `printer2`. Cada um executa em seu próprio processo/contêiner.
Somente computadores participam do Ricart–Agrawala e do gossip. As impressoras
recebem trabalhos por HTTP e simulam o tempo de impressão por página.

A participação é fixa durante uma execução. Todos os computadores conhecem os
mesmos membros e impressoras. Cada impressora tem uma instância independente
de RA; há um relógio lógico por computador e uma fila local por impressora.
Não há aquisição aninhada de impressoras nem eleição de coordenador.

A impressora registra e recusa tentativas simultâneas, sem enfileirá-las ou
conceder permissões. Isso é instrumentação: qualquer recusa por colisão é uma
falha de correção, mesmo que o dispositivo tenha impedido sobreposição física.
O verificador também verifica a exclusão das seções críticas nos computadores,
independentemente da recusa no dispositivo.

## Tempo e canais

Algoritmos assíncronos: a segurança não depende de relógios físicos sincronizados
ou limite de atraso. As condições de vivacidade incluem entrega eventual e
agendamento justo. Nos ensaios, os atrasos são configurados; esses parâmetros
não são uma prova de detecção correta de falhas.

HTTP/JSON sobre TCP, com deadline explícito (`RPC_TIMEOUT`). Não é suposta FIFO
entre RPCs: tarefas/conexões diferentes podem reordenar mensagens. Um timeout
pode ocorrer depois de o receptor processar a mensagem, portanto retransmissões
podem duplicar pedidos. As falhas de aplicação descartam chamadas antes do envio;
falhas reais de conexão também são tratadas como resultado incerto.

REQUEST inclui impressora, ID único do pedido, timestamp original e relógio de
mensagem. Retransmissões preservam ID e timestamp de prioridade. A ordem é o par
`(timestamp, node_id)`. IDs são comparados lexicograficamente, de maneira igual
em todos os participantes. Para os IDs de 1 a 5 isso coincide com a ordem numérica.
REPLY é uma mensagem explícita; a resposta HTTP ao REQUEST não concede acesso.
Respostas só contam para o pedido corrente e um remetente não conta duas vezes.
Pedidos e respostas são retransmitidos enquanto necessário, sem excluir membros.

O servidor não espera a liberação da impressora para concluir a RPC de REQUEST.
Ele registra o pedido adiado e envia REPLY em uma nova RPC após liberar o recurso.
A fila de respostas em memória tenta novamente até receber confirmação HTTP.

## Falhas, reinicialização e persistência

Suportadas: atraso, perdas, duplicação, partições, suspensão temporária e crash-stop
de computadores/impressoras. Não há falhas bizantinas, autenticação ou nós que
mintam sobre posse de permissões. O endpoint administrativo de falhas é uma
ferramenta de laboratório, fora do protocolo dos algoritmos.

Depois de um crash não há reentrada individual nesta versão. Reiniciar requer
encerrar todos os processos e iniciar uma execução completa. Restaurar apenas
um participante pode perder permissões adiadas, relógios e histórico e invalidar
as hipóteses de correção. `up` sempre recria o ambiente completo.

O histórico, os estados RA, trabalhos e deduplicação das impressoras estão em
memória. Cada processo escreve seu próprio JSONL; nenhum processo lê os arquivos
de outro nó. Compartilhar o volume de evidências no Compose não fornece um canal
de coordenação. Reinicialização não restaura esses arquivos.

A impressão usa ID único por trabalho, preservado em todas as consultas/reenvios.
O endpoint da impressora responde imediatamente com `printing` ou `completed`.
Reenviar um ID com o mesmo conteúdo consulta o mesmo trabalho; conteúdo diferente
é recusado. Não existe garantia de deduplicação entre reinicializações.

Enquanto o resultado da impressão é incerto, o computador mantém a seção crítica.
Isso pode bloquear a impressora indefinidamente se o dispositivo cair. Um
computador que cai após obter permissões também pode bloquear novos pedidos.
Nenhuma expiração equivale a liberação ou autorização.

## Segurança e vivacidade de Ricart–Agrawala

**Segurança:** para cada impressora, no máximo um computador está em `HELD`.
Para entrar, ele precisa de REPLY de todos os outros membros. Se dois pedidos
competem, a ordem total entre eles faz pelo menos um dos participantes adiar sua
resposta ao outro até sair. Portanto ambos não podem reunir todas as permissões
e estar na seção crítica ao mesmo tempo. Cada nó serializa seus próprios pedidos
por recurso com um lock local; esse lock não substitui o algoritmo entre nós.

Retransmissão, reordenação e duplicação não acrescentam membros nem mudam a
prioridade de um pedido. Respostas para IDs antigos são ignoradas. A lógica não
usa suspeitas ou gossip para dispensar permissões. Durante partições ou crashes,
a segurança permanece; o progresso pode parar.

**Vivacidade:** com todos os participantes ativos, processamento justo,
comunicação eventualmente bem-sucedida e seções críticas finitas, um pedido
recebe as permissões e termina. Pedidos novos têm relógios maiores que pedidos
já observados. Não se promete progresso permanente sob crash-stop/partição.

**Custo:** o protocolo básico requer `2(N-1)` mensagens por aquisição. Esta
implementação adiciona confirmações HTTP e retransmissões; `ra_messages` conta
as tentativas de REQUEST/REPLY, não os ACKs HTTP. O polling da fila de respostas
pode motivar retransmissão mesmo em rede sem perdas. A espera também depende da
carga e do número de pedidos anteriores para a mesma impressora.

## Segurança e vivacidade de Gossip

Cada registro concluído é imutável e indexado por `job_id` globalmente único.
Cada computador publica esses registros na própria entrada versionada, junto
com os dados de exemplo. Publicar uma impressão incrementa a versão; atualizar
os dados de exemplo preserva todos os registros. Snapshots novos não podem
remover ou alterar impressões já conhecidas. Snapshots antigos são ignorados;
conteúdo diferente com a mesma versão é erro. Um lote inválido é rejeitado antes
de modificar o estado. Não há recuperação individual a partir de réplicas.

O histórico apresentado pela aplicação é a união por ID dos registros dessas
entradas: associativa, comutativa e idempotente para registros válidos.
Conteúdos diferentes para um mesmo ID são erro. Somente o computador que recebeu
a confirmação de impressão cria o registro; os demais o aprendem por
anti-entropia. RPCs duplicadas não duplicam o histórico.

Cada rodada escolhe até `GOSSIP_FANOUT` computadores aleatórios por semente
(padrão 1), envia as entradas completas e as versões conhecidas e incorpora
as entradas mais novas retornadas. As respostas omitem versões que o solicitante
já conhece. A rodada aguarda as trocas, limitando RPCs simultâneas ao fanout, e
espera `GOSSIP_INTERVAL` mais até 10% de jitter. A finalização cancela e aguarda
as trocas em curso antes de fechar o cliente HTTP. Erros de uma troca são
registrados e não impedem rodadas posteriores.

`seconds_until_all_known` mede a primeira descoberta de todos os donos; não
comprova que as versões ou impressões posteriores já convergiram. As métricas
experimentais de convergência continuam usando os eventos `record_learned` por
impressão e a igualdade do histórico em todas as réplicas.

**Segurança:** um registro conhecido não muda nem é removido; mensagens
duplicadas não criam outra impressão nem outra entrada no histórico. A
idempotência física vale durante a vida da impressora simulada.

**Vivacidade:** depois de cessarem atualizações, com conectividade restaurada,
entrega eventual e escolhas justas de pares, todas as réplicas corretas convergem
para a união dos registros ainda presentes em pelo menos uma réplica correta.
Com seleção aleatória, convergência é probabilística e não tem prazo fixo. Na
partição, componentes podem divergir; após `heal`, voltam a trocar registros.

Um registro pode desaparecer se todas as suas cópias caírem; uma impressão pode
ficar sem histórico replicado se seu computador cair antes de divulgar a
confirmação. Não há atomicidade entre efeito físico e publicação distribuída.
Não se afirma consistência forte ou snapshot simultâneo das réplicas.

**Custo:** cada computador envia até F RPCs por rodada, para fanout efetivo F
limitado a N-1. Uma rodada de todos os computadores usa O(NF) mensagens e até
O(NFS) bytes, sendo S o tamanho serializado do estado completo, incluindo
histórico, dados de exemplo e metadados. As respostas podem ser menores ao
omitir versões já conhecidas. Ainda não há deltas por registro, paginação ou
compactação do histórico; não se promete custo logarítmico de bytes.

## Observação e ensaios

Logs de envio, pedidos RA, entrada/saída, começo/fim da impressão e aprendizado
de registros permitem verificar correção e medir custos separados. Os testes
exercitam rede sem perdas, perda/duplicação, respostas antigas, partições,
falhas e independência de recursos. Os cenários normais esperam todos os pedidos
concluídos e todas as réplicas idênticas; falhas permanentes esperam bloqueio.

`mono_ns` é o relógio monotônico do kernel compartilhado pelos processos e pelos
contêineres Docker padrão na máquina de referência, sem time namespaces. Sua
comparação entre hosts diferentes não é válida. `ts` usa tempo de parede apenas
para leitura humana; a coordenação usa exclusivamente o relógio lógico.

Contêineres no mesmo computador são nós lógicos, não domínios de falha física
independentes. O backend local compartilha ainda mais recursos e não substitui a
validação de reprodução em Compose. Sementes estabilizam escolhas aleatórias,
mas escalonamento do SO e interleaving de RPCs continuam não determinísticos;
por isso os ensaios usam várias sementes e reportam dispersão.
