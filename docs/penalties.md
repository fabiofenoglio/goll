# Uncompliance penalties

- [Penalize clients hitting the load limit](#penalize-clients-hitting-the-load-limit)
- [Penalize requests during overload status](#penalize-requests-during-overload-status)
- [Penalty distribution](#penalty-distribution)
- [Penalty cap](#penalty-cap)
- [Not sure?](#not-sure)

The limiting policy can be tuned in order to penalize clients for hitting the load limit and/or penalize clients that send an excessive number of requests and do not comply with the required delays.

With a couple parameters you can easily provide a better protection for your system that also encourages compliance and further limits uncompliant clients.

## Penalize clients hitting the load limit

You can create a limiter instance with the `OverstepPenaltyFactor` parameter valued higher than 0.

If you do, everytime a request gets rejected because the load limit was reached, a virtual amount of "penalty load" gets added to the current active load so that the effective cooldown before requests are accepted again will be slightly longer, depending on the value of the parameter.

The "penalty load" amount is computed as ` MaxLoad * OverstepPenaltyFactor`.

In the following example:

```go
limiter, err := goll.New(&goll.Config{
    MaxLoad:               100,
    WindowSize:            20 * time.Second,
    OverstepPenaltyFactor: 0.2,
})
```

everytime a client reaches the maximum load of 100 and requests more, the request will be rejected and the current active load will spike from 100 to `100 + (100 * 0.2) = 120`.

Small values of `OverstepPenaltyFactor` can help in keeping load under control for uncompliant clients while allowing compliant clients to go unrestricted.

For instance, with an `OverstepPenaltyFactor` of 0.2:
- to a compliant client, keeping its requests consistently under the maximum load, up to 100% of the MaxLoad will be accepted.
- to an uncompliant client consistently requesting more than the maximum allowed and not waiting the required delay amounts, only about 80% of the maximum load will be served (averaging).

## Penalize requests during overload status

You can choose to apply penalties every time a request is submitted and rejected when the maximum load was already reached and the previous request was already rejected with a `RetryIn` indication, before the required time has passed.

In this situation, the client knows that your system is overloaded, received a "please wait" response but is not waiting as requested: it may be a good idea to penalize the uncompliancy in order to protect your system and encourage proper limiting implementation on client-side.

You can do so by passing a `RequestOverheadPenaltyFactor` parameter higher than 0 to the constructor. If you do so, every time a request is rejected in such a situation, a virtual penalty load of `RequestOverheadPenaltyFactor * the requested load` is applied.

In the following example:

```go
limiter, err := goll.New(&goll.Config{
    MaxLoad:                      100,
    WindowSize:                   20 * time.Second,
    RequestOverheadPenaltyFactor: 0.5,
})
```

a request for a load of 10 while the current load is already 100/100 and a "please wait" response was already sent will incur in a virtual load penalty of `10 * 0.5 = 5`.

Small values of `RequestOverheadPenaltyFactor` can help in protecting against uncompliant clients, DDoS attempts or poor retry implementations without proper backoff on client side.

## Penalty distribution

By default, virtual load penalties are added to the current segment of the sliding window (the most recent one).

You can distribute the load penalties over a higher portion of the window in order to have a smoother cooldown by providing a `OverstepPenaltyDistributionFactor` or `RequestOverheadPenaltyDistributionFactor` parameter between 0 and 1.0 to the constructor:

For instance, in the following example:

```go
limiter, err := goll.New(&goll.Config{
    MaxLoad:                                  100,
    WindowSize:                               20 * time.Second,
    OverstepPenaltyFactor:                    0.2,
    OverstepPenaltyDistributionFactor:        0.33,
    RequestOverheadPenaltyFactor:             0.5,
    RequestOverheadPenaltyDistributionFactor: 0.33,
})
```

every penalty will be distributed in the segments composing the most recent `20 seconds * 0.33 = 6.6 seconds` of the window so that the cooldown will be slightly smoother.

## Penalty cap

In case an aggressive penalyzing policy is applied you could risk having a penalized active load so high that it will take too long to cooldown,
effectively cutting out the client from service entirely.

To prevent this you can (and should, if you are using some kind of penalty) provide the `MaxPenaltyCapFactor` to the constructor.
If you do, no matter how many penalties get applied, the current active load will never be allowed to go over the maximum treshold of:
`MaxLoad * (1.0 + MaxPenaltyCapFactor)`.

In the following example:

```go
limiter, err := goll.New(&goll.Config{
    MaxLoad:                                  100,
    WindowSize:                               20 * time.Second,
    OverstepPenaltyFactor:                    0.2,
    OverstepPenaltyDistributionFactor:        0.33,
    MaxPenaltyCapFactor:                      0.5,
})
```

The active load will never be allowed to go over `100 * (1.0 + 0.5) = 150`.

## Not sure?

If you are not sure of the parameters, the following are a good starting point to start experimenting:

```go
limiter, err := goll.New(&goll.Config{
    MaxLoad:                            100,
    WindowSize:                         20 * time.Second,
    OverstepPenaltyFactor:              0.10,
    OverstepPenaltyDistributionFactor:  0.25,
    MaxPenaltyCapFactor:                0.5,
})
```
