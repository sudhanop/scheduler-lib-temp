package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestScheduler_Registration(t *testing.T) {
	s := New()

	dummyTask := func(ctx context.Context) error { return nil }
	sched, _ := Every(1 * time.Minute)

	// Empty name
	if err := s.Register("", sched, dummyTask); !errors.Is(err, ErrEmptyJobName) {
		t.Errorf("expected ErrEmptyJobName, got %v", err)
	}

	// Nil schedule
	if err := s.Register("job1", nil, dummyTask); !errors.Is(err, ErrNilSchedule) {
		t.Errorf("expected ErrNilSchedule, got %v", err)
	}

	// Nil task
	if err := s.Register("job1", sched, nil); !errors.Is(err, ErrNilTask) {
		t.Errorf("expected ErrNilTask, got %v", err)
	}

	// Successful registration
	if err := s.Register("job1", sched, dummyTask); err != nil {
		t.Fatalf("unexpected registration error: %v", err)
	}

	// Duplicate registration
	if err := s.Register("job1", sched, dummyTask); !errors.Is(err, ErrDuplicateJob) {
		t.Errorf("expected ErrDuplicateJob, got %v", err)
	}

	// Verify job count and info
	if s.Count() != 1 {
		t.Errorf("expected count 1, got %d", s.Count())
	}
	info, err := s.Job("job1")
	if err != nil {
		t.Fatalf("unexpected error finding job1: %v", err)
	}
	if info.Name != "job1" {
		t.Errorf("expected name job1, got %q", info.Name)
	}

	// Remove job
	if err := s.Remove("job1"); err != nil {
		t.Fatalf("unexpected error removing job1: %v", err)
	}
	if s.Count() != 0 {
		t.Errorf("expected count 0 after removal, got %d", s.Count())
	}

	// Remove non-existent job
	if err := s.Remove("job1"); !errors.Is(err, ErrJobNotFound) {
		t.Errorf("expected ErrJobNotFound, got %v", err)
	}
}

func TestScheduler_IntervalExecution(t *testing.T) {
	s := New()

	var count atomic.Int32
	task := func(ctx context.Context) error {
		count.Add(1)
		return nil
	}

	// 15ms interval
	if err := s.RegisterInterval("ticker", 15*time.Millisecond, task); err != nil {
		t.Fatalf("registration failed: %v", err)
	}

	if err := s.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	// Allow several intervals to fire
	time.Sleep(70 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if err := s.Stop(ctx); err != nil {
		t.Fatalf("stop failed: %v", err)
	}

	if count.Load() < 3 {
		t.Errorf("expected at least 3 executions, got %d", count.Load())
	}

	info, err := s.Job("ticker")
	if err != nil {
		t.Fatalf("job lookup failed: %v", err)
	}
	if info.RunCount < 3 {
		t.Errorf("expected runCount >= 3, got %d", info.RunCount)
	}
	if info.LastRun.IsZero() {
		t.Errorf("expected LastRun to be set")
	}
}

func TestScheduler_ImmediateFirstRun(t *testing.T) {
	s := New()

	fired := make(chan struct{}, 1)
	task := func(ctx context.Context) error {
		select {
		case fired <- struct{}{}:
		default:
		}
		return nil
	}

	// 1 hour interval, but with immediate first run
	if err := s.RegisterInterval("immediate-job", 1*time.Hour, task, WithImmediateFirstRun()); err != nil {
		t.Fatalf("registration failed: %v", err)
	}

	if err := s.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		_ = s.Stop(ctx)
	}()

	select {
	case <-fired:
		// Succeeded immediately without waiting 1 hour
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("immediate first run did not fire within expected duration")
	}
}

func TestScheduler_MultipleSimultaneousJobs(t *testing.T) {
	s := New()

	var wg sync.WaitGroup
	wg.Add(3)

	taskMaker := func(id int) Task {
		return func(ctx context.Context) error {
			wg.Done()
			return nil
		}
	}

	_ = s.RegisterInterval("job-1", 1*time.Hour, taskMaker(1), WithImmediateFirstRun())
	_ = s.RegisterInterval("job-2", 1*time.Hour, taskMaker(2), WithImmediateFirstRun())
	_ = s.RegisterInterval("job-3", 1*time.Hour, taskMaker(3), WithImmediateFirstRun())

	if err := s.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		_ = s.Stop(ctx)
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All 3 simultaneous jobs executed
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("timed out waiting for simultaneous jobs")
	}
}

func TestScheduler_TaskErrorHandling(t *testing.T) {
	var reportedJob string
	var reportedErr error
	var errMu sync.Mutex

	customErr := errors.New("simulated task error")

	s := New(WithErrorHandler(func(name string, err error) {
		errMu.Lock()
		reportedJob = name
		reportedErr = err
		errMu.Unlock()
	}))

	task := func(ctx context.Context) error {
		return customErr
	}

	if err := s.RegisterInterval("failing-job", 1*time.Hour, task, WithImmediateFirstRun()); err != nil {
		t.Fatalf("registration failed: %v", err)
	}

	if err := s.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("stop failed: %v", err)
	}

	errMu.Lock()
	defer errMu.Unlock()

	if reportedJob != "failing-job" {
		t.Errorf("expected reported job 'failing-job', got %q", reportedJob)
	}
	if !errors.Is(reportedErr, customErr) {
		t.Errorf("expected %v, got %v", customErr, reportedErr)
	}

	info, _ := s.Job("failing-job")
	if !errors.Is(info.LastError, customErr) {
		t.Errorf("expected job LastError to be %v, got %v", customErr, info.LastError)
	}
}

func TestScheduler_ConcurrencyPrevention(t *testing.T) {
	var skipped atomic.Bool

	s := New(WithErrorHandler(func(name string, err error) {
		if errors.Is(err, ErrJobAlreadyRunning) {
			skipped.Store(true)
		}
	}))

	var concurrentRuns atomic.Int32
	var maxConcurrent atomic.Int32

	// A task that takes 60ms to complete
	slowTask := func(ctx context.Context) error {
		current := concurrentRuns.Add(1)
		defer concurrentRuns.Add(-1)

		for {
			oldMax := maxConcurrent.Load()
			if current <= oldMax || maxConcurrent.CompareAndSwap(oldMax, current) {
				break
			}
		}

		time.Sleep(60 * time.Millisecond)
		return nil
	}

	// Register with a fast 15ms interval. Since default is non-concurrent,
	// subsequent ticks while slowTask is running must be skipped!
	if err := s.RegisterInterval("slow-job", 15*time.Millisecond, slowTask, WithImmediateFirstRun()); err != nil {
		t.Fatalf("registration failed: %v", err)
	}

	if err := s.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	// Wait 80ms for at least 3-4 interval ticks to attempt dispatch
	time.Sleep(80 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("stop failed: %v", err)
	}

	if maxConcurrent.Load() > 1 {
		t.Errorf("expected maximum concurrent instances <= 1, got %d", maxConcurrent.Load())
	}
	if !skipped.Load() {
		t.Errorf("expected at least one skipped run to be reported to ErrorHandler")
	}
}

func TestScheduler_GracefulShutdown(t *testing.T) {
	s := New()

	var taskFinished atomic.Bool
	task := func(ctx context.Context) error {
		time.Sleep(50 * time.Millisecond)
		taskFinished.Store(true)
		return nil
	}

	_ = s.RegisterInterval("graceful-job", 1*time.Hour, task, WithImmediateFirstRun())
	_ = s.Start()

	// Wait a moment so the job has started executing
	time.Sleep(10 * time.Millisecond)

	// Stop with generous timeout
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	if err := s.Stop(ctx); err != nil {
		t.Fatalf("expected clean stop, got %v", err)
	}

	if !taskFinished.Load() {
		t.Errorf("expected in-flight task to finish cleanly before Stop returned")
	}
}

func TestScheduler_ShutdownContextTimeout(t *testing.T) {
	s := New()

	task := func(ctx context.Context) error {
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
		}
		return nil
	}

	_ = s.RegisterInterval("blocking-job", 1*time.Hour, task, WithImmediateFirstRun())
	_ = s.Start()

	time.Sleep(10 * time.Millisecond)

	// Stop with tight timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := s.Stop(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestScheduler_LifecycleErrors(t *testing.T) {
	s := New()

	// Stopping before starting
	ctx := context.Background()
	if err := s.Stop(ctx); !errors.Is(err, ErrSchedulerStopped) {
		t.Errorf("expected ErrSchedulerStopped, got %v", err)
	}

	// Start
	if err := s.Start(); err != nil {
		t.Fatalf("unexpected start error: %v", err)
	}

	// Start again while running
	if err := s.Start(); !errors.Is(err, ErrSchedulerRunning) {
		t.Errorf("expected ErrSchedulerRunning, got %v", err)
	}

	// Stop cleanly
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("unexpected stop error: %v", err)
	}

	// Stop again after stopped
	if err := s.Stop(ctx); !errors.Is(err, ErrSchedulerStopped) {
		t.Errorf("expected ErrSchedulerStopped, got %v", err)
	}
}

func TestScheduler_DynamicRegistrationWhileRunning(t *testing.T) {
	s := New()

	if err := s.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	done := make(chan struct{})
	task := func(ctx context.Context) error {
		close(done)
		return nil
	}

	// Register while running with immediate run
	if err := s.RegisterInterval("dynamic-job", 1*time.Hour, task, WithImmediateFirstRun()); err != nil {
		t.Fatalf("registration while running failed: %v", err)
	}

	select {
	case <-done:
		// Woke up and executed successfully
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("dynamically registered job did not execute")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_ = s.Stop(ctx)
}
