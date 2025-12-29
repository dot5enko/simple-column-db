package lightsync

import (
	"sync/atomic"
	"time"
)

type Waiter struct {
	erHappened atomic.Bool
	err        error
	tasks      atomic.Int32
	wasted     atomic.Int32

	sleepStep time.Duration
}

func (w *Waiter) Done() {
	newValue := w.tasks.Add(-1)
	if newValue < 0 {
		panic("negative waiter value")
	}
}

func (w *Waiter) Add(n int) {
	w.tasks.Add(int32(n))
}

func (w *Waiter) Errored(err error) {
	w.err = err
	w.erHappened.Store(true)
}

func (w *Waiter) SetSleepStep(step time.Duration) { w.sleepStep = step }

func (w *Waiter) WastedCycles() int32 { return w.wasted.Load() }

// wait
// returns error if any of the tasks failed
func (w *Waiter) Wait() error {

	for {
		if w.erHappened.Load() {
			return w.err
		}

		currentTasks := w.tasks.Load()
		if currentTasks == 0 {
			break
		}

		w.wasted.Add(1)
		time.Sleep(w.sleepStep)
	}

	return nil
}
