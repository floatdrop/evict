module github.com/floatdrop/evict/bench

go 1.26.0

require (
	github.com/floatdrop/evict v0.0.0-00010101000000-000000000000
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/klauspost/compress v1.20.4
	github.com/maypok86/otter/v2 v2.4.0
	github.com/ulikunitz/xz v0.5.17
)

replace github.com/floatdrop/evict => ../
