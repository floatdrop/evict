# Memory

What a cache takes up for every entry it has room for, in bytes. A cache is set up the
way its library suggests and written to until it is full, with four times as many keys
as it has room for. The live heap is measured before and after.

Produced by `go run ./cmd/memory -capacities 10000,100000,1000000,10000000 -out results` with go1.27.3 on darwin/arm64.

## Integers

Keys and values are `uint64`. The 16 bytes they take are part of the number, since the
caches of this module store them in place.

| cache | 10,000 | 100,000 | 1,000,000 | 10,000,000 |
|:---|---:|---:|---:|---:|
| evict/lru | 47.5 | 57.9 | 49.3 | 45.5 |
| evict/twoq | 79.6 | 73.6 | 81.8 | 74.9 |
| evict/sieve | 67.9 | 71.4 | 59.8 | 53.7 |
| evict/s3fifo | 82.2 | 89.9 | 75.6 | 67.7 |
| otter | 94.5 | 83.6 | 90.5 | 89.2 |
| golang-lru | 109.6 | 127.3 | 117.8 | 110.3 |
| golang-lru/2q | 156.4 | 182.9 | 168.8 | 157.4 |

## Strings

Keys and values are strings that exist before the cache does, so the number is the
overhead of the cache alone, including the 32 bytes of the two string headers.

| cache | 10,000 | 100,000 | 1,000,000 | 10,000,000 |
|:---|---:|---:|---:|---:|
| evict/lru | 66.2 | 73.5 | 65.0 | 61.4 |
| evict/twoq | 105.8 | 94.6 | 105.9 | 98.9 |
| evict/sieve | 83.4 | 87.1 | 77.1 | 69.8 |
| evict/s3fifo | 97.8 | 105.6 | 92.9 | 83.8 |
| otter | 106.5 | 97.6 | 106.5 | 105.2 |
| golang-lru | 139.7 | 165.9 | 151.9 | 140.8 |
| golang-lru/2q | 201.6 | 240.9 | 219.9 | 203.1 |

## Garbage collection

What a full cache of integers gives the garbage collector to do. The collector does not
look into memory that holds no pointers, which is all there is to a cache of this module
when its keys and values have none.

The first table has the part of the heap that the collector has to look through because
of the cache, in bytes for every entry the cache has room for. It hardly differs from
one run to the next.

| cache | 10,000 | 100,000 | 1,000,000 | 10,000,000 |
|:---|---:|---:|---:|---:|
| evict/lru | 0.7 | 0.2 | 0.0 | 0.0 |
| evict/twoq | 0.9 | 0.2 | 0.0 | 0.0 |
| evict/sieve | 1.5 | 0.3 | 0.0 | 0.0 |
| evict/s3fifo | 1.7 | 0.4 | 0.0 | 0.0 |
| otter | 48.7 | 40.6 | 50.1 | 43.8 |
| golang-lru | 93.5 | 111.3 | 101.8 | 94.3 |
| golang-lru/2q | 136.3 | 162.9 | 148.7 | 137.4 |

The second has the CPU time that one collection takes while the cache is alive and
little else is, in milliseconds: the least and the most of seven collections. It is the
time of all the CPUs that took part, so it does not shrink with their number as the wait
for a collection does. Whatever else the machine is doing adds to it, which is why the
lower number is the one to go by.

| cache | 10,000 | 100,000 | 1,000,000 | 10,000,000 |
|:---|---:|---:|---:|---:|
| evict/lru | 0.36–0.56 | 0.22–0.91 | 0.19–0.30 | 0.20–0.29 |
| evict/twoq | 0.34–0.53 | 0.28–0.42 | 0.34–0.63 | 0.20–0.36 |
| evict/sieve | 0.37–0.51 | 0.33–0.46 | 0.33–0.47 | 0.17–0.26 |
| evict/s3fifo | 0.37–0.49 | 0.36–0.56 | 0.30–0.46 | 0.19–0.33 |
| otter | 0.66–0.82 | 5.28–6.05 | 63.91–68.87 | 630.93–643.89 |
| golang-lru | 1.42–2.10 | 8.66–9.36 | 65.09–70.80 | 736.45–853.82 |
| golang-lru/2q | 1.65–2.02 | 11.53–12.97 | 99.34–108.11 | 1228.51–1268.25 |
