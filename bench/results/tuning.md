# Tuning S3-FIFO

What `evict/s3fifo` answers of a trace as it is, and what it would answer with other
numbers for its queues. The cache is as the S3-FIFO paper has it unless its options say
otherwise: a tenth of the room for the small queue, nine tenths as many ghosts as entries,
two reads to move up from the small queue, and a ghost that goes to the main queue the
first time it comes back. Here it is replayed with 84 settings: the small queue at 5%, 10%,
20%, 30%, 50%, 70% and 90% of the capacity, 0.9, 3 and 6 times as many ghosts as entries,
one or two reads to move up, and one or two returns of a ghost.

The caches are in one piece. A setting is written as the options that make it:
`small` is WithSmallRatio, `ghosts` WithGhostRatio, `promotion` WithPromotionReads and
`returns` WithGhostReturns. Where settings do equally well, the standard one is named
before the others, and one with fewer ghosts before one with more: a ghost takes about
sixteen bytes.

Produced by `go run ./cmd/tune -out results` with go1.27.3.

## At a glance

The hit ratio in percent, averaged over the capacities at which a trace was replayed.

| trace | as it is | one setting for all | best setting for the trace | best setting for each capacity | the best setting for the trace is |
|:---|---:|---:|---:|---:|:---|
| Zipf | 38.25 | 37.86 | 38.79 | 39.03 | small 0.05, ghosts 3, promotion 1, returns 1 |
| S3 | 38.90 | 40.18 | 41.75 | 43.05 | small 0.05, ghosts 0.9, promotion 1, returns 2 |
| DS1 | 37.05 | 43.06 | 45.46 | 45.57 | small 0.05, ghosts 0.9, promotion 1, returns 2 |
| OLTP | 39.11 | 38.45 | 40.64 | 40.71 | small 0.2, ghosts 6, promotion 2, returns 2 |
| P8 | 35.31 | 35.99 | 37.11 | 37.62 | small 0.05, ghosts 0.9, promotion 2, returns 2 |
| Glimpse | 43.24 | 44.67 | 44.84 | 44.91 | small 0.05, ghosts 0.9, promotion 1, returns 2 |
| Mixed | 43.84 | 56.37 | 59.53 | 60.54 | small 0.1, ghosts 6, promotion 2, returns 1 |
| Scarab | 61.60 | 60.72 | 61.71 | 62.18 | small 0.2, ghosts 0.9, promotion 2, returns 1 |
| Twitter | 60.32 | 60.11 | 60.47 | 60.52 | small 0.1, ghosts 3, promotion 2, returns 1 |
| MSR | 67.22 | 67.14 | 67.23 | 67.42 | small 0.1, ghosts 0.9, promotion 1, returns 1 |
| CloudPhysics | 27.59 | 27.46 | 27.70 | 28.42 | small 0.05, ghosts 0.9, promotion 2, returns 1 |
| **mean** | 44.77 | 46.55 | 47.75 | 48.18 | |

The one setting that does best for all of these traces together is: small 0.05, ghosts 3, promotion 2, returns 2.

## Trace by trace

### Zipf

1,000,000 requests.

| capacity | as it is | best | with |
|---:|---:|---:|:---|
| 500 | 24.41 | 24.96 | small 0.05, ghosts 6, promotion 2, returns 2 |
| 1,000 | 28.70 | 29.20 | small 0.05, ghosts 6, promotion 2, returns 2 |
| 2,000 | 32.88 | 33.39 | small 0.05, ghosts 6, promotion 1, returns 2 |
| 5,000 | 38.00 | 38.51 | small 0.05, ghosts 6, promotion 1, returns 2 |
| 10,000 | 41.39 | 42.15 | small 0.05, ghosts 3, promotion 1, returns 1 |
| 20,000 | 44.32 | 45.51 | small 0.05, ghosts 3, promotion 1, returns 1 |
| 40,000 | 46.90 | 48.30 | small 0.05, ghosts 6, promotion 1, returns 1 |
| 80,000 | 49.40 | 50.17 | small 0.05, ghosts 6, promotion 1, returns 1 |

### S3

16,407,702 requests.

| capacity | as it is | best | with |
|---:|---:|---:|:---|
| 100,000 | 12.34 | 12.86 | small 0.05, ghosts 0.9, promotion 1, returns 1 |
| 200,000 | 22.52 | 23.47 | small 0.05, ghosts 0.9, promotion 1, returns 1 |
| 300,000 | 29.81 | 31.53 | small 0.05, ghosts 0.9, promotion 2, returns 1 |
| 400,000 | 34.62 | 40.25 | small 0.05, ghosts 0.9, promotion 1, returns 2 |
| 500,000 | 41.42 | 49.31 | small 0.05, ghosts 0.9, promotion 1, returns 2 |
| 600,000 | 49.21 | 56.23 | small 0.05, ghosts 0.9, promotion 2, returns 2 |
| 700,000 | 56.97 | 62.50 | small 0.05, ghosts 0.9, promotion 1, returns 2 |
| 800,000 | 64.31 | 68.22 | small 0.05, ghosts 0.9, promotion 1, returns 2 |

### DS1

43,704,979 requests.

| capacity | as it is | best | with |
|---:|---:|---:|:---|
| 1,000,000 | 14.09 | 15.21 | small 0.05, ghosts 3, promotion 1, returns 2 |
| 2,000,000 | 25.32 | 27.83 | small 0.05, ghosts 0.9, promotion 1, returns 2 |
| 3,000,000 | 30.85 | 38.59 | small 0.05, ghosts 0.9, promotion 1, returns 2 |
| 4,000,000 | 31.96 | 43.87 | small 0.05, ghosts 0.9, promotion 1, returns 2 |
| 5,000,000 | 37.31 | 50.23 | small 0.05, ghosts 0.9, promotion 1, returns 2 |
| 6,000,000 | 40.03 | 56.51 | small 0.05, ghosts 0.9, promotion 1, returns 2 |
| 7,000,000 | 51.97 | 62.36 | small 0.05, ghosts 0.9, promotion 1, returns 2 |
| 8,000,000 | 64.91 | 69.98 | small 0.05, ghosts 0.9, promotion 1, returns 2 |

### OLTP

914,145 requests.

| capacity | as it is | best | with |
|---:|---:|---:|:---|
| 250 | 21.65 | 26.30 | small 0.3, ghosts 3, promotion 2, returns 2 |
| 500 | 32.40 | 35.15 | small 0.3, ghosts 3, promotion 2, returns 2 |
| 750 | 37.70 | 39.29 | small 0.2, ghosts 6, promotion 2, returns 2 |
| 1,000 | 40.82 | 41.85 | small 0.2, ghosts 6, promotion 2, returns 2 |
| 1,250 | 42.91 | 43.73 | small 0.2, ghosts 6, promotion 2, returns 2 |
| 1,500 | 44.53 | 45.24 | small 0.2, ghosts 6, promotion 2, returns 2 |
| 1,750 | 45.88 | 46.51 | small 0.2, ghosts 6, promotion 2, returns 2 |
| 2,000 | 46.99 | 47.64 | small 0.2, ghosts 6, promotion 2, returns 2 |

### P8

42,243,785 requests.

| capacity | as it is | best | with |
|---:|---:|---:|:---|
| 10,000 | 12.29 | 13.18 | small 0.05, ghosts 3, promotion 2, returns 2 |
| 20,000 | 21.20 | 22.39 | small 0.05, ghosts 3, promotion 2, returns 2 |
| 30,000 | 28.23 | 30.47 | small 0.05, ghosts 0.9, promotion 2, returns 2 |
| 40,000 | 34.43 | 37.21 | small 0.05, ghosts 0.9, promotion 2, returns 2 |
| 50,000 | 39.62 | 42.73 | small 0.05, ghosts 0.9, promotion 2, returns 2 |
| 60,000 | 44.67 | 47.48 | small 0.05, ghosts 0.9, promotion 2, returns 2 |
| 70,000 | 49.02 | 51.90 | small 0.05, ghosts 0.9, promotion 2, returns 2 |
| 80,000 | 53.01 | 55.64 | small 0.05, ghosts 0.9, promotion 2, returns 2 |

### Glimpse

6,015 requests.

| capacity | as it is | best | with |
|---:|---:|---:|:---|
| 250 | 15.83 | 16.46 | small 0.05, ghosts 3, promotion 1, returns 1 |
| 500 | 30.54 | 32.65 | small 0.05, ghosts 0.9, promotion 1, returns 1 |
| 750 | 39.09 | 41.06 | small 0.05, ghosts 0.9, promotion 2, returns 2 |
| 1,000 | 44.34 | 49.06 | small 0.05, ghosts 0.9, promotion 2, returns 2 |
| 1,250 | 50.42 | 52.59 | small 0.1, ghosts 0.9, promotion 1, returns 2 |
| 1,500 | 53.00 | 53.55 | small 0.05, ghosts 0.9, promotion 2, returns 2 |
| 1,750 | 55.33 | 55.98 | small 0.5, ghosts 0.9, promotion 2, returns 1 |
| 2,000 | 57.41 | 57.96 | small 0.05, ghosts 0.9, promotion 2, returns 1 |

### Mixed

776,904 requests.

| capacity | as it is | best | with |
|---:|---:|---:|:---|
| 250 | 9.50 | 21.82 | small 0.2, ghosts 6, promotion 2, returns 2 |
| 500 | 11.70 | 33.87 | small 0.2, ghosts 3, promotion 2, returns 2 |
| 750 | 11.73 | 49.60 | small 0.3, ghosts 3, promotion 2, returns 1 |
| 1,000 | 11.76 | 72.61 | small 0.05, ghosts 3, promotion 2, returns 1 |
| 1,250 | 76.44 | 76.59 | small 0.9, ghosts 0.9, promotion 2, returns 1 |
| 1,500 | 76.49 | 76.61 | small 0.7, ghosts 0.9, promotion 2, returns 1 |
| 1,750 | 76.54 | 76.61 | small 0.7, ghosts 0.9, promotion 2, returns 1 |
| 2,000 | 76.58 | 76.64 | small 0.5, ghosts 0.9, promotion 2, returns 1 |

### Scarab

1,245,184 requests.

| capacity | as it is | best | with |
|---:|---:|---:|:---|
| 1,000 | 35.22 | 35.91 | small 0.1, ghosts 6, promotion 2, returns 2 |
| 5,000 | 47.75 | 47.93 | small 0.1, ghosts 6, promotion 2, returns 2 |
| 10,000 | 53.51 | 53.57 | small 0.05, ghosts 6, promotion 2, returns 2 |
| 25,000 | 62.41 | 62.58 | small 0.05, ghosts 3, promotion 2, returns 1 |
| 50,000 | 69.47 | 70.14 | small 0.3, ghosts 0.9, promotion 1, returns 1 |
| 75,000 | 72.99 | 73.93 | small 0.3, ghosts 0.9, promotion 1, returns 1 |
| 100,000 | 75.03 | 76.05 | small 0.3, ghosts 0.9, promotion 1, returns 1 |
| 125,000 | 76.40 | 77.30 | small 0.3, ghosts 0.9, promotion 1, returns 1 |

### Twitter

2,245,093 requests.

| capacity | as it is | best | with |
|---:|---:|---:|:---|
| 1,000 | 53.52 | 53.62 | small 0.1, ghosts 3, promotion 2, returns 1 |
| 2,500 | 55.78 | 56.03 | small 0.1, ghosts 6, promotion 2, returns 1 |
| 5,000 | 57.55 | 57.75 | small 0.1, ghosts 6, promotion 2, returns 1 |
| 10,000 | 59.13 | 59.35 | small 0.2, ghosts 3, promotion 2, returns 1 |
| 25,000 | 61.29 | 61.71 | small 0.2, ghosts 3, promotion 2, returns 1 |
| 50,000 | 63.13 | 63.34 | small 0.1, ghosts 3, promotion 2, returns 1 |
| 100,000 | 65.00 | 65.08 | small 0.1, ghosts 3, promotion 2, returns 1 |
| 200,000 | 67.17 | 67.25 | small 0.05, ghosts 3, promotion 2, returns 1 |

### MSR

3,993,316 requests.

| capacity | as it is | best | with |
|---:|---:|---:|:---|
| 1,000 | 53.99 | 54.04 | small 0.1, ghosts 3, promotion 1, returns 1 |
| 2,500 | 57.43 | 58.01 | small 0.2, ghosts 3, promotion 1, returns 1 |
| 5,000 | 60.59 | 60.68 | small 0.2, ghosts 0.9, promotion 1, returns 1 |
| 10,000 | 63.46 | 63.73 | small 0.05, ghosts 6, promotion 2, returns 2 |
| 25,000 | 67.91 | 68.29 | small 0.05, ghosts 3, promotion 2, returns 2 |
| 50,000 | 72.17 | 72.29 | small 0.05, ghosts 0.9, promotion 1, returns 1 |
| 100,000 | 77.99 | 78.04 | small 0.05, ghosts 0.9, promotion 2, returns 1 |
| 200,000 | 84.24 | 84.26 | small 0.1, ghosts 3, promotion 2, returns 1 |

### CloudPhysics

3,375,344 requests.

| capacity | as it is | best | with |
|---:|---:|---:|:---|
| 1,000 | 13.65 | 13.65 | small 0.05, ghosts 0.9, promotion 2, returns 1 |
| 2,500 | 14.44 | 14.51 | small 0.05, ghosts 6, promotion 2, returns 1 |
| 5,000 | 15.22 | 16.03 | small 0.05, ghosts 6, promotion 2, returns 2 |
| 10,000 | 17.84 | 18.82 | small 0.05, ghosts 6, promotion 2, returns 2 |
| 25,000 | 27.62 | 27.82 | small 0.05, ghosts 0.9, promotion 2, returns 1 |
| 50,000 | 33.87 | 34.36 | small 0.2, ghosts 3, promotion 1, returns 2 |
| 100,000 | 43.28 | 43.43 | small 0.1, ghosts 0.9, promotion 1, returns 1 |
| 200,000 | 54.81 | 58.78 | small 0.1, ghosts 0.9, promotion 1, returns 2 |
