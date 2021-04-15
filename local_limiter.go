package main

import (
	"sync"
	"time"
)

type LocalLimiter struct {
	rate   float64
	volume float64
	mu     sync.Mutex
	m      map[string]float64
	t map[string]int64
}

func NewLocalLimiter(rate, volume int64) *LocalLimiter {
	l := new(LocalLimiter)
	l.rate = float64(rate) / 1000000000
	l.volume = float64(volume)
	l.m = make(map[string]float64)
	l.t = make(map[string]int64)
	return l
}

func (l *LocalLimiter) IsLimited(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	lastTokens, ok := l.m[key]
	if !ok {
		lastTokens = l.volume
	}
	ts := l.t[key]
	tn := time.Now().UnixNano()
	var delta int64
	if tn >= ts && ts != 0 {
		delta = tn - ts
	} else {
		delta = 0
	}
	var filledTokens float64
	incre := float64(delta) * l.rate
	if l.volume <= lastTokens+incre {
		filledTokens = l.volume
	} else {
		filledTokens = lastTokens + incre
	}
	allowed := filledTokens >= 1.0
	if allowed {
		filledTokens -= 1
	}
	l.m[key] = filledTokens
	l.t[key] = tn
	return !allowed
}
