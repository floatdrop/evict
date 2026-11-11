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
| evict/sieve | 67.9 | 71.5 | 59.8 | 53.7 |
| evict/s3fifo | 82.3 | 89.9 | 75.6 | 67.7 |
| evict/tinylfu | 81.4 | 82.0 | 68.2 | 67.2 |
| otter | 94.0 | 82.9 | 90.5 | 89.2 |
| golang-lru | 109.6 | 127.3 | 117.8 | 110.3 |
| golang-lru/2q | 156.4 | 182.9 | 168.8 | 157.5 |

## Strings

Keys and values are strings that exist before the cache does, so the number is the
overhead of the cache alone, including the 32 bytes of the two string headers.

| cache | 10,000 | 100,000 | 1,000,000 | 10,000,000 |
|:---|---:|---:|---:|---:|
| evict/lru | 66.2 | 73.5 | 65.0 | 61.4 |
| evict/twoq | 105.8 | 94.6 | 105.9 | 98.9 |
| evict/sieve | 83.4 | 87.1 | 77.1 | 69.8 |
| evict/s3fifo | 97.8 | 105.6 | 92.9 | 83.8 |
| evict/tinylfu | 97.0 | 97.7 | 85.5 | 83.3 |
| otter | 106.5 | 97.6 | 106.4 | 105.2 |
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
| evict/twoq | 0.7 | 0.2 | 0.0 | 0.0 |
| evict/sieve | 1.6 | 0.3 | 0.0 | 0.0 |
| evict/s3fifo | 1.9 | 0.4 | 0.0 | 0.0 |
| evict/tinylfu | 1.9 | 0.4 | 0.0 | 0.0 |
| otter | 48.3 | 40.1 | 50.1 | 43.8 |
| golang-lru | 93.4 | 111.3 | 101.8 | 94.3 |
| golang-lru/2q | 136.3 | 162.9 | 148.7 | 137.4 |

The second has the CPU time that one collection takes while the cache is alive and
little else is, in milliseconds: the least and the most of seven collections. It is the
time of all the CPUs that took part, so it does not shrink with their number as the wait
for a collection does. Whatever else the machine is doing adds to it, which is why the
lower number is the one to go by.

| cache | 10,000 | 100,000 | 1,000,000 | 10,000,000 |
|:---|---:|---:|---:|---:|
| evict/lru | 0.36–0.63 | 0.27–0.53 | 0.16–0.31 | 0.20–0.34 |
| evict/twoq | 0.36–0.53 | 0.35–0.56 | 0.35–0.47 | 0.22–0.39 |
| evict/sieve | 0.39–0.51 | 0.28–0.55 | 0.42–0.53 | 0.19–0.33 |
| evict/s3fifo | 0.43–0.56 | 0.33–1.03 | 0.34–0.61 | 0.23–0.37 |
| evict/tinylfu | 0.39–0.61 | 0.36–0.48 | 0.38–0.56 | 0.19–0.38 |
| otter | 0.67–1.12 | 5.94–6.60 | 64.28–68.37 | 633.72–653.00 |
| golang-lru | 1.30–1.61 | 8.50–9.00 | 64.80–67.87 | 740.43–863.89 |
| golang-lru/2q | 1.26–2.21 | 11.84–12.96 | 97.42–102.44 | 1167.18–1230.22 |
