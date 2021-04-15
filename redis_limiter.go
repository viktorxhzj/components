package main

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	REDIS_TIMESTAMP_SUFFIX = ":timestamp"
)

type RedisLimiter struct {
	Rate float64
	Volume int64
	rdb *redis.Client
	ctx context.Context
}

func NewRedisLimiter(ctx context.Context, redisAddr string, rate, volume int64) *RedisLimiter {
	r := new(RedisLimiter)
	r.Rate = float64(rate)/1000000000
	r.Volume = volume
	r.rdb = redis.NewClient(&redis.Options{
        Addr:     redisAddr,
        Password: "",
        DB:       0,
    })
	r.ctx = ctx
	return r
}

func (r *RedisLimiter) IsLimited(key string) bool {
	keys := []string{
		key,
		key+REDIS_TIMESTAMP_SUFFIX,
	}
	args := []interface{}{
		r.Rate,
		r.Volume,
		time.Now().UnixNano(),
		1,
	}

	res, err := r.rdb.Eval(r.ctx, LUA_SCRIPT, keys, args).Result()
	if err != nil {
		panic(err)
	}
	n := res.([]interface{})[0].(int64)
	return n == 0
}

const LUA_SCRIPT = `
--令牌桶剩余令牌数key
local tokens_key = KEYS[1] 
--令牌桶最后填充时间key
local timestamp_key = KEYS[2]

--往令牌桶投放令牌速率 
local rate = tonumber(ARGV[1])
--令牌桶大小
local capacity = tonumber(ARGV[2])
--当前数据戳
local now = tonumber(ARGV[3])
--请求获取令牌数量
local requested = tonumber(ARGV[4])
--计算令牌桶填充满需要的时间
local fill_time = capacity/rate/1000000000
--保证时间充足
local ttl = math.floor(fill_time*2) + 1
--获取redis中剩余令牌数
local last_tokens = tonumber(redis.call("get", tokens_key))
if last_tokens == nil then
  last_tokens = capacity
end
--获取redis中最后一次更新令牌的时间
local last_refreshed = tonumber(redis.call("get", timestamp_key))
if last_refreshed == nil then
  last_refreshed = 0
end

local delta = math.max(0, now-last_refreshed)
--计算出需要更新redis里的令牌桶数量（通过 过去的时间间隔内需要投放的令牌数+桶剩余令牌）
local filled_tokens = math.min(capacity, last_tokens+(delta*rate))
local allowed = filled_tokens >= requested
local new_tokens = filled_tokens
local allowed_num = 0
--消耗令牌后，重新计算出需要更新redis缓存里的令牌数
if allowed then
  new_tokens = filled_tokens - requested
  allowed_num = 1
end
--互斥更新redis 里的剩余令牌数
redis.call("setex", tokens_key, ttl, new_tokens)
--互斥更新redis 里的最新更新令牌时间
redis.call("setex", timestamp_key, ttl, now)

return { allowed_num, new_tokens }
`