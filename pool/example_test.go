package pool_test

import (
	"context"
	"fmt"

	pool "github.com/Diudiuseven/work-stealing-pool-go/pool"
)

func Example() {
	p := pool.New(pool.Config{
		Workers:       4,
		QueueCapacity: 64,
		Policy:        pool.OverflowBlock,
		StarveEvery:   8,
	})
	task, err := p.SubmitTask(context.Background(), pool.PriorityHigh,
		func(ctx context.Context) error {
			fmt.Println("hello from the pool")
			return nil
		})
	if err != nil {
		panic(err)
	}
	<-task.Done()
	p.Shutdown()
	p.Drain()
	// Output: hello from the pool
}
