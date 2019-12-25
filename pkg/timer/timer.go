package timer

import (
	"time"
	"sync"
	"github.com/ngaut/log"
)

// Timer struct
type Timer struct {
	sync.Mutex
	n          int64
	m          int64
	lastBatchN int64
	checkPoint time.Time
	startPoint time.Time
}

// New create Timer
func New() *Timer {
	return &Timer{
		checkPoint: time.Now(),
		startPoint: time.Now(),
	}
}

// Add 1
func (t *Timer) Add() {
	t.Lock()
	defer t.Unlock()
	t.n++
	t.autoCheck()
}

// Addn add n at once
func (t *Timer) Addn(n int64) {
	t.Lock()
	defer t.Unlock()
	t.n += n
	t.autoCheck()
}

// SetAutoCheck set up an auto check up by calc mod
func (t *Timer) SetAutoCheck(m int64) {
	t.m = m
}

func (t *Timer) autoCheck() {
	if t.m == 0 {
		return
	}
	if t.n % t.m == 0 {
		t.check()
	}
}

func (t *Timer) check() {
	now := time.Now()
	t.PrintQPS(now)
	t.checkPoint = now
	t.lastBatchN = t.n
}

// Check insert point and print QPS
func (t *Timer) Check() {
	t.Lock()
	defer t.Unlock()
	t.check()
}

// PrintQPS log QPS
func (t *Timer) PrintQPS(now time.Time) {
	var (
		batchDuration = int64(now.Sub(t.checkPoint)/time.Millisecond) + 1
		totalDuration = int64(now.Sub(t.startPoint)/time.Millisecond) + 1
		batchQPS = int64((t.n - t.lastBatchN)*1000/batchDuration)
		totalQPS = int64(t.n*1000/totalDuration)
	)
	log.Infof("Done %d, Batch QPS = %d, Total QPS = %d", t.n, batchQPS, totalQPS)
}
