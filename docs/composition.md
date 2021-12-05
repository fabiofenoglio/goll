# Composition

It is often useful to combine multiple constraints on the acceptable load.

For instance you may want to limit the maximum load your system can handle every minute but you'd also like to limit the load per second in order to protect against sudden request bursts.

### Create a composite LoadLimiter instance

Use the `NewComposite` function to create a composed instance. 

The following example creates a limiter accepting a maximum load of 100 over a window of 30 seconds **AND** limiting a maximum load of 10 over a single second.

```go
limiter, err := goll.NewComposite(&goll.CompositeConfig{
    Limiters: []goll.Config{
        {
            MaxLoad:           100,
            WindowSize:        30 * time.Second,
        }, 
        {
            MaxLoad:           10,
            WindowSize:        1 * time.Second,
        },
    },
})

if err != nil {
    panic(err)
}

// usage is the same as the standard limiter
```

Please not that both the regular limiter and the composite limiter implement the `goll.LoadLimiter` interface to allow for easy transition between the limit modes. 

You are encoraged to use `goll.LoadLimiter` as type when you store references to your limiters.