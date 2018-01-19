# limit

```
package limit // import "github.com/mjolnir42/limit"

Package limit implements a concurrency limit.

func New(parallel uint32) *Limit
    New returns a new concurrency limit that allows parallel concurrent
    executions

type Limit struct {
        // Has unexported fields.
}
    Limit can be used to limit concurrency on a resource to a specific number of
    goroutines, for example the number of active in-flight HTTP requests.

    l := limit.New(4)
    ...
    go func() {
        l.Start()
        defer l.Done()
        ... use resource ...
    }()

    Not calling Done() will over time starve l and render the limit permanently
    reached, blocking all Start() requests.

func (l *Limit) Start()
    Start signals that the caller wants to utilize the a resource guarded by l.
    It blocks until the caller is free to use the resource. The caller must call
    Done() once finished.

func (l *Limit) Done()
    Done signals that the caller is finished using the resource guarded by
    Limit. It decrements the usage and wakes up all goroutines waiting on its
    availability.
```
