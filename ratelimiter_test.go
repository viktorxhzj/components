package main

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type RateLimiter interface {
	IsLimited(string) bool
}

var (
	HandlerSize  = 5
	InstanceSize = 3
)

func TestRedisLimiter(t *testing.T) {
	keys := make([]string, HandlerSize)
	for i := 0; i < HandlerSize; i++ {
		keys[i] = fmt.Sprintf("Key%d", i)
	}
	chans := make([]chan int, HandlerSize)
	ctx := context.Background()

	for i := 0; i < HandlerSize; i++ {
		chans[i] = make(chan int)
		go InitServer(keys[i], chans[i])
	}
	for i := 0; i < InstanceSize; i++ {
		go InitClient(i, keys, chans, NewRedisLimiter(ctx, "localhost:6379", 60, 10))
	}
	time.Sleep(5 * time.Second)
}

func TestLocalLimiter_SingleInstance(t *testing.T) {
	keys := make([]string, 1)
	for i := 0; i < 1; i++ {
		keys[i] = fmt.Sprintf("Key%d", i)
	}
	chans := make([]chan int, 1)

	for i := 0; i < 1; i++ {
		chans[i] = make(chan int)
		go InitServer(keys[i], chans[i])
	}
	for i := 0; i < 1; i++ {
		go InitClient(i, keys, chans, NewLocalLimiter(60, 10))
	}
	time.Sleep(5 * time.Second)
}

func TestLocalLimiter_MultiInstances(t *testing.T) {
	keys := make([]string, HandlerSize)
	for i := 0; i < HandlerSize; i++ {
		keys[i] = fmt.Sprintf("Key%d", i)
	}
	chans := make([]chan int, HandlerSize)

	for i := 0; i < HandlerSize; i++ {
		chans[i] = make(chan int)
		go InitServer(keys[i], chans[i])
	}
	for i := 0; i < InstanceSize; i++ {
		go InitClient(i, keys, chans, NewLocalLimiter(60, 10))
	}
	time.Sleep(5 * time.Second)
}

func InitServer(key string, ch chan int) {
	var counter int
	ts := time.Now()
	for {
		<-ch
		counter++
		tn := time.Now()
		d := tn.Sub(ts).Seconds()

		if d >= 1.0 {
			rate := float64(counter) / d
			fmt.Printf("KEY=%s, RATE=%.2f\n", key, rate)
			ts = tn
			counter = 0
		}
	}
}

func InitClient(cli int, keys []string, chans []chan int, limiter RateLimiter) {
	interval := 10
	n := len(keys)

	// 模拟一个实例的多个路由
	for i := 0; i < n; i++ {
		go func(idx int) {
			ts := time.Now()
			var counter int
			for {
				time.Sleep(time.Duration(interval) * time.Millisecond)
				if limiter.IsLimited(keys[idx]) {
					continue
				}
				chans[idx] <- 0
				counter++
				tn := time.Now()
				d := tn.Sub(ts).Seconds()
				if d >= 1.0 {
					// rate := float64(counter) / d
					// fmt.Printf("[CLI %d]KEY=%s, RATE=%.2f\n", cli, keys[idx], rate)
					counter = 0
					ts = tn
				}
			}
		}(i)
	}
}
