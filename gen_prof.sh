#!/bin/bash

curl http://localhost:6060/debug/pprof/allocs?seconds=10 > allocate.pprof
curl http://localhost:6060/debug/pprof/block?seconds=10 > block.pprof
curl http://localhost:6060/debug/pprof/cmdline?seconds=10 > cmdline.pprof
curl http://localhost:6060/debug/pprof/goroutine?seconds=10 > goroutine.pprof
curl http://localhost:6060/debug/pprof/heap?seconds=10 > heap.pprof
curl http://localhost:6060/debug/pprof/mutex?seconds=10 > mutex.pprof
curl http://localhost:6060/debug/pprof/profile?seconds=10 > cpu.pprof
curl http://localhost:6060/debug/pprof/threadcreate?seconds=10 > threadcreate.pprof
curl http://localhost:6060/debug/pprof/trace?seconds=10 > trace.pprof