package taskque

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

type TaskResult struct {
	Name     string
	Value    interface{}
	Error    error
	Canceled bool
}

type Task struct {
	Name       string
	Group      string
	Priority   int
	Timeout    time.Duration
	Context    context.Context
	Do         func(ctx context.Context) (interface{}, error)
	ResultChan chan TaskResult

	created time.Time
}

type TaskQueue struct {
	maxWorkers int
	maxQueue   int

	queue          []*Task
	running        int
	runningByGroup map[string]int
	mu             sync.Mutex
	cond           *sync.Cond
	shutdown       bool
	wg             sync.WaitGroup
}

var (
	ErrQueueFull    = errors.New("queue is full")
	ErrQueueStopped = errors.New("queue is stopped")
)

func New(maxWorkers, maxQueue int) *TaskQueue {
	tq := &TaskQueue{
		maxWorkers:     maxWorkers,
		maxQueue:       maxQueue,
		queue:          make([]*Task, 0),
		runningByGroup: make(map[string]int),
	}

	tq.cond = sync.NewCond(&tq.mu)

	go tq.loop()

	return tq
}

func (tq *TaskQueue) loop() {
	for {
		tq.mu.Lock()
		for !tq.shutdown && (len(tq.queue) == 0 || tq.running >= tq.maxWorkers) {
			tq.cond.Wait()
		}

		if tq.shutdown && len(tq.queue) == 0 && tq.running == 0 {
			tq.mu.Unlock()

			return
		}

		if len(tq.queue) == 0 || tq.running >= tq.maxWorkers {
			tq.mu.Unlock()

			continue
		}

		task := tq.queue[0]

		tq.queue = tq.queue[1:]
		tq.running++
		tq.runningByGroup[task.Group]++
		tq.mu.Unlock()

		go tq.runTask(task)
	}
}

func (tq *TaskQueue) runTask(task *Task) {
	tq.wg.Add(1)

	defer func() {
		tq.wg.Done()
		tq.mu.Lock()
		tq.running--
		tq.runningByGroup[task.Group]--
		tq.mu.Unlock()
		tq.cond.Broadcast()
	}()

	ctx := task.Context
	if ctx == nil {
		ctx = context.Background()
	}

	if task.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, task.Timeout)
		defer cancel()
	}

	var result interface{}
	var err error

	done := make(chan struct{})

	go func() {
		defer close(done)

		result, err = task.Do(ctx)
	}()

	select {
	case <-ctx.Done():
		if task.ResultChan != nil {
			select {
			case task.ResultChan <- TaskResult{
				Name:     task.Name,
				Value:    nil,
				Error:    ctx.Err(),
				Canceled: true,
			}:
			default:
			}
		}
	case <-done:
		if task.ResultChan != nil {
			select {
			case task.ResultChan <- TaskResult{
				Name:     task.Name,
				Value:    result,
				Error:    err,
				Canceled: false,
			}:
			default:
			}
		}
	}
}

func (tq *TaskQueue) insertTask(task *Task, index int) error {
	if tq.shutdown {
		return ErrQueueStopped
	}

	if len(tq.queue) >= tq.maxQueue {
		return ErrQueueFull
	}

	if task.created.IsZero() {
		task.created = time.Now()
	}

	if index < 0 || index > len(tq.queue) {
		tq.queue = append(tq.queue, task)
	} else {
		tq.queue = append(tq.queue[:index], append([]*Task{task}, tq.queue[index:]...)...)
	}

	tq.sortQueue()
	tq.cond.Broadcast()

	return nil
}

func (tq *TaskQueue) sortQueue() {
	sort.SliceStable(tq.queue, func(i, j int) bool {
		if tq.queue[i].Priority != tq.queue[j].Priority {
			return tq.queue[i].Priority < tq.queue[j].Priority
		}

		return tq.queue[i].created.Before(tq.queue[j].created)
	})
}

func (tq *TaskQueue) EnqueueSync(task *Task) TaskResult {
	resultChan := make(chan TaskResult, 1)
	task.ResultChan = resultChan

	err := tq.Enqueue(task)
	if err != nil {
		return TaskResult{
			Name:     task.Name,
			Value:    nil,
			Error:    err,
			Canceled: true,
		}
	}

	return <-resultChan
}

func (tq *TaskQueue) Enqueue(task *Task) error {
	tq.mu.Lock()
	defer tq.mu.Unlock()

	return tq.insertTask(task, -1)
}

func (tq *TaskQueue) EnqueueFront(task *Task) error {
	tq.mu.Lock()
	defer tq.mu.Unlock()

	return tq.insertTask(task, 0)
}

func (tq *TaskQueue) EnqueueAfterGroup(task *Task, group string) error {
	tq.mu.Lock()
	defer tq.mu.Unlock()

	if tq.shutdown {
		return ErrQueueStopped
	}

	if len(tq.queue) >= tq.maxQueue {
		return ErrQueueFull
	}

	if task.created.IsZero() {
		task.created = time.Now()
	}

	index := -1
	for i := len(tq.queue) - 1; i >= 0; i-- {
		if tq.queue[i].Group == group {
			index = i + 1
			break
		}
	}

	return tq.insertTask(task, index)
}

func (tq *TaskQueue) EnqueueBeforeGroup(task *Task, group string) error {
	tq.mu.Lock()
	defer tq.mu.Unlock()

	if tq.shutdown {
		return ErrQueueStopped
	}

	if len(tq.queue) >= tq.maxQueue {
		return ErrQueueFull
	}

	if task.created.IsZero() {
		task.created = time.Now()
	}

	index := -1
	for i := 0; i < len(tq.queue); i++ {
		if tq.queue[i].Group == group {
			index = i
			break
		}
	}

	return tq.insertTask(task, index)
}

func (tq *TaskQueue) RunningCount() int {
	return tq.running
}

func (tq *TaskQueue) GroupRunningCount(group string) int {
	tq.mu.Lock()
	defer tq.mu.Unlock()

	return tq.runningByGroup[group]
}

func (tq *TaskQueue) QueueSize() int {
	tq.mu.Lock()
	defer tq.mu.Unlock()

	return len(tq.queue)
}

func (tq *TaskQueue) MaxQueueSize() int {
	return tq.maxQueue
}

func (tq *TaskQueue) Stop() {
	tq.mu.Lock()
	tq.shutdown = true
	tq.mu.Unlock()
	tq.cond.Broadcast()
}

func (tq *TaskQueue) WaitAll() {
	tq.wg.Wait()
}
