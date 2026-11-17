# Experimental Results: 15 Computers, 5 Printers

## Setup

The experiment matrix ran with Docker Compose, using 15 computers and 5
printers for each of three scenarios: normal operation, 20% message loss, and
a full partition. Each scenario ran with seeds 11, 22, and 33. Every trial
measured 45 jobs (three per computer) and included one separate warmup job. The
loss scenario also applied 10 ms of delay. In the partition scenario, an
application-level partition separated 14 computers from the remaining
computer for 0.4 s; it was then healed so blocked jobs could finish and records
could converge.

The results below show mean ± sample standard deviation across the three seeds.
For latency metrics, p50, p95, and p99 were calculated per seed and then
summarized across seeds. Acquisition latency includes waiting in the local
queue. Gossip convergence is measured from a record's creation at its origin
computer until the last computer learns it. Message counts include attempts
and retries. Byte counts cover application JSON requests and responses,
excluding transport headers.

## Ricart–Agrawala

| Scenario | Mean acquisition (ms) | p50 (ms) | p95 (ms) | p99 (ms) | REQUEST/REPLY attempts | Application bytes |
|---|---:|---:|---:|---:|---:|---:|
| Normal | 920.65 ± 23.21 | 927.45 ± 41.74 | 1,556.51 ± 14.74 | 1,607.89 ± 3.50 | 5,481 ± 155 | 961,455 ± 27,131 |
| 20% loss | 1,222.16 ± 109.94 | 1,226.66 ± 144.59 | 2,000.15 ± 168.75 | 2,086.87 ± 161.04 | 6,758 ± 365 | 1,165,545 ± 63,019 |
| Full partition | 1,367.70 ± 41.34 | 1,367.23 ± 38.14 | 2,013.01 ± 26.50 | 2,074.67 ± 45.46 | 8,919 ± 54 | 1,560,505 ± 11,225 |

## Gossip

| Scenario | Mean convergence (ms) | p50 (ms) | p95 (ms) | p99 (ms) | Push-pull attempts | Application bytes |
|---|---:|---:|---:|---:|---:|---:|
| Normal | 260.71 ± 4.81 | 253.16 ± 18.46 | 347.84 ± 35.71 | 385.59 ± 21.07 | 319 ± 20 | 2,545,462 ± 285,273 |
| 20% loss | 358.30 ± 2.49 | 347.53 ± 13.23 | 508.14 ± 7.69 | 573.76 ± 16.73 | 394 ± 21 | 3,004,593 ± 203,859 |
| Full partition | 258.30 ± 11.19 | 255.71 ± 9.20 | 351.09 ± 14.79 | 376.56 ± 5.16 | 398 ± 8 | 2,669,443 ± 112,315 |

## System throughput

| Scenario | Throughput (jobs/s) |
|---|---:|
| Normal | 21.47 ± 1.02 |
| 20% loss | 16.76 ± 0.94 |
| Full partition | 17.66 ± 0.36 |
