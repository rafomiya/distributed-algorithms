# Experimental Results: 15 Computers, 5 Printers

## Setup

The experiment matrix ran with Docker Compose, using 15 computers and 5
printers for each of three scenarios: normal operation, 20% message loss, and
a full partition. Each scenario ran with seeds 11, 22, and 33. Every trial
measured 45 jobs (three per computer) and included one separate warmup job. The
loss scenario also applied 10 ms of delay. In the partition scenario, an
application-level partition separated computers 1–7 from computers 8–15
for 0.4 s; it was then healed so blocked jobs could finish and records
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
| Normal | 978.48 ± 5.31 | 988.84 ± 13.40 | 1,663.72 ± 20.62 | 1,722.64 ± 14.95 | 5,782 ± 105 | 1,014,232 ± 19,598 |
| 20% loss | 1,148.39 ± 33.25 | 1,145.91 ± 31.49 | 1,915.62 ± 89.14 | 1,992.43 ± 87.19 | 6,731 ± 329 | 1,160,815 ± 57,076 |
| Full partition | 1,418.27 ± 60.66 | 1,454.21 ± 81.54 | 2,070.27 ± 66.24 | 2,155.28 ± 54.32 | 11,286 ± 650 | 1,948,722 ± 113,334 |

## Gossip

| Scenario | Mean convergence (ms) | p50 (ms) | p95 (ms) | p99 (ms) | Push-pull attempts | Application bytes |
|---|---:|---:|---:|---:|---:|---:|
| Normal | 258.31 ± 9.86 | 255.65 ± 22.97 | 346.67 ± 5.33 | 369.41 ± 15.86 | 326 ± 4 | 2,562,018 ± 68,667 |
| 20% loss | 374.83 ± 16.93 | 367.32 ± 21.90 | 498.06 ± 2.87 | 543.76 ± 33.11 | 390 ± 3 | 2,996,280 ± 74,406 |
| Full partition | 258.08 ± 10.78 | 257.91 ± 8.90 | 345.38 ± 34.02 | 371.91 ± 37.46 | 413 ± 9 | 2,731,481 ± 51,044 |

## System throughput

| Scenario | Throughput (jobs/s) |
|---|---:|
| Normal | 20.79 ± 0.44 |
| 20% loss | 17.13 ± 0.05 |
| Full partition | 17.22 ± 0.42 |