package factorygo

import (
	"context"
	"fmt"
	"sync"
)

type Factory struct {
	maxQueueSize   int
	maxWorkers     int
	workerJobsChan chan Job
	cancelJobFuncs map[int]context.CancelFunc
	mu             sync.Mutex
	waitGroup      sync.WaitGroup // Used to wait for all jobs to finish
}

func NewFactory(maxQueueSize, maxWorkers int) *Factory {
	return &Factory{
		maxQueueSize:   maxQueueSize,
		maxWorkers:     maxWorkers,
		workerJobsChan: make(chan Job, maxQueueSize),
		cancelJobFuncs: make(map[int]context.CancelFunc),
	}
}

func (f *Factory) Start() {
	for i := 1; i <= f.maxWorkers; i++ {
		go f.worker(i)
	}
}

func (f *Factory) WaitAll() {
	f.waitGroup.Wait() // Wait for all jobs to finish
}

func (f *Factory) Stop() {
	// Cancel all running jobs
	f.mu.Lock()
	for _, cancel := range f.cancelJobFuncs {
		cancel()
	}
	f.mu.Unlock()

	// Wait for all running jobs to finish before returning
	f.WaitAll()
}

func (f *Factory) worker(workerID int) error {
	for job := range f.workerJobsChan {
		ctx, cancel := context.WithCancel(context.Background())
		f.storeJob(job.ID, cancel)
		f.waitGroup.Add(1) // Increment the WaitGroup counter
		err := job.Execute(ctx, workerID)
		if err != nil {
			return err
		}
		f.waitGroup.Done() // Decrement the WaitGroup counter when the job is done
		f.cleanupJob(job.ID)
	}

	return nil
}

func (f *Factory) AddJob(job Job) error {
	select {
	case f.workerJobsChan <- job:
		return nil
	default:
		return fmt.Errorf("job queue is full")
	}
}

type Job struct {
	ID       int
	Executor func(ctx context.Context) error // Function to be executed
}

func (j *Job) Execute(ctx context.Context, workerID int) error {
	select {
	case <-ctx.Done():
		return nil
	default:
		fmt.Printf("--- end search job %d ---\n", j.ID)
	}

	// Call the provided executor function
	if j.Executor != nil {
		return j.Executor(ctx)
	}

	return nil
}

func (f *Factory) storeJob(id int, cancel context.CancelFunc) {
	f.mu.Lock()
	f.cancelJobFuncs[id] = cancel
	f.mu.Unlock()
}

func (f *Factory) cleanupJob(id int) {
	f.mu.Lock()
	delete(f.cancelJobFuncs, id)
	f.mu.Unlock()
}

func (f *Factory) CancelJob(id int) error {
	f.mu.Lock()
	cancelFunc, ok := f.cancelJobFuncs[id]
	if !ok {
		f.mu.Unlock()
		return fmt.Errorf("no cancel function found")
	}
	cancelFunc()
	delete(f.cancelJobFuncs, id)
	f.mu.Unlock()
	return nil
}
