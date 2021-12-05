# Synchronization

- [What and Why](#what-and-why)
- [However](#however)
- [Get the adapter module](#get-the-adapter-module)
- [Create a Redis pool](#create-a-redis-pool)
- [Create the adapter](#create-the-adapter)
- [Create a LoadLimiter instance with the adapter](#create-a-loadlimiter-instance-with-the-adapter)
- [Full sample](#full-sample)
- [Bring your own adapter](#bring-your-own-adapter)

### What and Why

In order to handle heavy loads you will probably be horizontally scaling your application.

The goll load limiter is able to quickly work in such a scenario by synchronizing live load data across all the required instances using the method you prefer.

A sample implementation is provided to synchronize via a Redis instance ([goll-redis](https://github.com/fabiofenoglio/goll-redis)) but you could write your own adapter for memcached, any database, an infinispan cluster and so on.


```go
limiter, err := goll.New(&goll.Config{
    MaxLoad:           1000,
    WindowSize:        20 * time.Second,

    // pass your adapter in the constructor.
    SyncAdapter:       yourAdapter
})

```

### However

The fact that you can do it doesn't necessarily mean that you should:

synchronizing many instances significantly increases the overhead per-request and many times you don't actually need it.

Horizontally scaling means that you distribute the load across many instances,
so in many cases it would be enough to use standalone load limiters without synchronization to limit the load on each instance separately.

### Get the adapter module

We'll go over a sample configuration for synchronizing over a Redis instance.

First get the adapter module for Redis by running:

```bash
go get github.com/fabiofenoglio/goll-redis
```

The implementation requires you to bring your own Redis pool so that you can 
plug in your preferred Redis client library.

You may use `go-redis` for instance:

```bash
go get github.com/go-redis/redis/v8
```

### Create a Redis pool

Remember to import goll, the goll-redis adapter **and** the client library for Redis.

Your imports should be similar to these:

```go
import (
	goll "github.com/fabiofenoglio/goll"
	gollredis "github.com/fabiofenoglio/goll-redis"
	goredislib "github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
)
```

You can start by creating a Redis client and a pool:


```go
client := goredislib.NewClient(&goredislib.Options{
    Addr:     "localhost:6379",
})

pool := goredis.NewPool(client)
```

### Create the adapter

Use the pool you just created to get an instance of the goll-redis adapter:

```go
adapter, err := gollredis.NewRedisSyncAdapter(&gollredis.Config{
    Pool:      pool,
    MutexName: "redisAdapterTest",
})
```

### Create a LoadLimiter instance with the adapter

Now just pass the adapter to the `New` method and your load limiter will synchronize with other identical instances connected to the same Redis instance:

```go
limiter, err := goll.New(&goll.Config{
    MaxLoad:           1000,
    WindowSize:        20 * time.Second,
    SyncAdapter:       adapter, // the adapter we just created
})
```

You can now use your instance and enjoy automatic synchronization.

You may notice some new logs regarding sync transactions, look up for any warnings or errors because by default synchronization errors are not blocking.

```
...
2021/11/30 18:04:19 [info] [sync tx] acquiring lock
2021/11/30 18:04:19 [info] [sync tx] lock acquired
2021/11/30 18:04:19 [info] [sync tx] fetching status
2021/11/30 18:04:19 [info] [sync tx] fetched status
2021/11/30 18:04:19 [info] instance version is up to date with serialized data, nothing to do
2021/11/30 18:04:19 [info] [sync tx] executing task
2021/11/30 18:04:19 [info] [sync tx] writing updated status to remote store
2021/11/30 18:04:19 [info] [sync tx] end
2021/11/30 18:04:19 [info] [sync tx] releasing lock
2021/11/30 18:04:19 [info] [sync tx] lock released
request for load of 17 was accepted
2021/11/30 18:04:19 [info] [sync tx] acquiring lock
2021/11/30 18:04:19 [info] [sync tx] lock acquired
2021/11/30 18:04:19 [info] [sync tx] fetching status
2021/11/30 18:04:19 [info] [sync tx] fetched status
2021/11/30 18:04:19 [info] instance version is up to date with serialized data, nothing to do
2021/11/30 18:04:19 [info] [sync tx] executing task
2021/11/30 18:04:19 [info] [sync tx] end
2021/11/30 18:04:19 [info] [sync tx] releasing lock
2021/11/30 18:04:19 [info] [sync tx] lock released
limiter status: windowTotal=28 segments=[ 17  11], 2 requests processed
...
```

### Full sample

Please check out the 
[full example of cluster synchronization via a Redis instance](https://github.com/fabiofenoglio/goll-examples/redis%20synchronization/main.go).


### Bring your own adapter

You can provide your custom implementation, just make sure you implement the `goll.SyncAdapter` interface.

You essentialy have to implement four methods:

- `Lock` and `Unlock` which should work mutex-like
- `Fetch` which reads a string from store shared with the other instances
- `Write` which writes a string to the same shared store

You can check out the [goll-redis](https://github.com/fabiofenoglio/goll-redis) module as an example.
