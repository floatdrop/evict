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
| evict/lru | 61.4 | 63.1 | 56.6 | 56.0 |
| evict/twoq | 92.7 | 89.3 | 84.4 | 84.0 |
| evict/sieve | 81.4 | 76.7 | 67.2 | 64.3 |
| evict/s3fifo | 100.2 | 96.4 | 85.5 | 82.4 |
| evict/tinylfu | 94.9 | 87.3 | 75.6 | 77.8 |
| otter | 94.9 | 83.1 | 90.5 | 89.2 |
| golang-lru | 109.6 | 127.3 | 117.8 | 110.3 |
| golang-lru/2q | 156.4 | 182.9 | 168.8 | 157.4 |

## Strings

Keys and values are strings that exist before the cache does, so the number is the
overhead of the cache alone, including the 32 bytes of the two string headers.

| cache | 10,000 | 100,000 | 1,000,000 | 10,000,000 |
|:---|---:|---:|---:|---:|
| evict/lru | 79.6 | 78.8 | 72.4 | 72.0 |
| evict/twoq | 118.9 | 110.3 | 108.5 | 108.1 |
| evict/sieve | 96.9 | 92.4 | 84.5 | 80.4 |
| evict/s3fifo | 115.7 | 112.2 | 102.8 | 98.5 |
| evict/tinylfu | 110.4 | 103.0 | 92.9 | 93.9 |
| otter | 106.5 | 97.6 | 106.5 | 105.2 |
| golang-lru | 139.7 | 165.9 | 151.9 | 140.8 |
| golang-lru/2q | 201.6 | 240.9 | 219.9 | 203.2 |

## Garbage collection

What a full cache of integers gives the garbage collector to do. The collector does not
look into memory that holds no pointers, which is all there is to a cache of this module
when its keys and values have none.

The first table has the part of the heap that the collector has to look through because
of the cache, in bytes for every entry the cache has room for. It hardly differs from
one run to the next.

| cache | 10,000 | 100,000 | 1,000,000 | 10,000,000 |
|:---|---:|---:|---:|---:|
| evict/lru | 1.5 | 0.2 | 0.0 | 0.0 |
| evict/twoq | 0.9 | 0.2 | 0.0 | 0.0 |
| evict/sieve | 1.8 | 0.3 | 0.0 | 0.0 |
| evict/s3fifo | 2.2 | 0.4 | 0.0 | 0.0 |
| evict/tinylfu | 2.2 | 0.4 | 0.0 | 0.0 |
| otter | 49.0 | 40.3 | 50.1 | 43.8 |
| golang-lru | 93.5 | 111.3 | 101.8 | 94.3 |
| golang-lru/2q | 136.1 | 162.9 | 148.7 | 137.4 |

The second has the CPU time that one collection takes while the cache is alive and
little else is, in milliseconds: the least and the most of seven collections. It is the
time of all the CPUs that took part, so it does not shrink with their number as the wait
for a collection does. Whatever else the machine is doing adds to it, which is why the
lower number is the one to go by.

| cache | 10,000 | 100,000 | 1,000,000 | 10,000,000 |
|:---|---:|---:|---:|---:|
| evict/lru | 0.43–0.55 | 0.25–0.41 | 0.17–0.28 | 0.19–0.37 |
| evict/twoq | 0.34–0.48 | 0.32–0.60 | 0.34–0.49 | 0.22–0.34 |
| evict/sieve | 0.28–0.62 | 0.32–0.53 | 0.29–0.61 | 0.29–0.83 |
| evict/s3fifo | 0.32–0.74 | 0.48–1.20 | 0.46–0.98 | 0.24–0.40 |
| evict/tinylfu | 0.41–0.59 | 0.33–0.59 | 0.35–0.59 | 0.22–0.34 |
| otter | 0.63–0.88 | 5.48–6.25 | 65.18–69.80 | 530.03–584.60 |
| golang-lru | 1.25–1.69 | 7.34–9.69 | 66.21–72.52 | 719.26–828.59 |
| golang-lru/2q | 1.63–2.02 | 11.80–13.09 | 100.53–109.53 | 1157.10–1312.65 |
