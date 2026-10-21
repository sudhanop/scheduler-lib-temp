package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	scheduler "github.com/sudhanop/scheduler-lib-temp"
)

func main() {
	// CLI flags
	immediateFlag := flag.Bool("immediate", false, "Trigger an immediate run for all jobs upon startup for quick verification")
	flag.Parse()

	fmt.Println("================================================================================")
	fmt.Println("              rlib-scheduler: Production Go Scheduler Demonstration             ")
	fmt.Println("================================================================================")

	// 1. Initialize the Scheduler with an ErrorHandler
	sched := scheduler.New(
		scheduler.WithErrorHandler(func(jobName string, err error) {
			log.Printf("[SCHEDULER ALERT] Job %q encountered error: %v\n", jobName, err)
		}),
	)

	// Determine job registration options based on CLI flag
	var commonOpts []scheduler.JobOption
	if *immediateFlag {
		fmt.Println("[INFO] Immediate execution mode enabled: all jobs will run once upon startup.")
		commonOpts = append(commonOpts, scheduler.WithImmediateFirstRun())
	}

	// 2. Define Tasks for all 7 required schedules

	// Task 1: 30-Second interval
	task30s := func(ctx context.Context) error {
		fmt.Printf("[%s] [30s-Heartbeat]  Heartbeat check OK. System health: GREEN\n",
			time.Now().Format("15:04:05.000"))
		return nil
	}

	// Task 2: 1-Minute interval
	task1m := func(ctx context.Context) error {
		fmt.Printf("[%s] [1m-Metrics]     Aggregated CPU and memory usage metrics\n",
			time.Now().Format("15:04:05.000"))
		return nil
	}

	// Task 3: 10-Minute interval
	task10m := func(ctx context.Context) error {
		fmt.Printf("[%s] [10m-MarketScan] Market scan completed. 248 tickers processed\n",
			time.Now().Format("15:04:05.000"))
		return nil
	}

	// Task 4: Clock Hourly (at the beginning of every clock hour, e.g., 09:00:00, 10:00:00)
	taskHourly := func(ctx context.Context) error {
		fmt.Printf("[%s] [Hourly-Sync]    Top-of-the-hour synchronization dispatched\n",
			time.Now().Format("15:04:05.000"))
		return nil
	}

	// Task 5: Daily at a specified time (09:00:00 local time)
	taskDaily := func(ctx context.Context) error {
		fmt.Printf("[%s] [Daily-Digest]   Daily briefing report compiled and published\n",
			time.Now().Format("15:04:05.000"))
		return nil
	}

	// Task 6: Monthly on a specified day and time (1st day of month at 00:00:00)
	taskMonthly := func(ctx context.Context) error {
		fmt.Printf("[%s] [Monthly-Bill]   Monthly billing cycle statements generated\n",
			time.Now().Format("15:04:05.000"))
		return nil
	}

	// Task 7: Yearly on a specified month, day, and time (January 1st at 00:00:00)
	taskYearly := func(ctx context.Context) error {
		fmt.Printf("[%s] [Yearly-Audit]   Annual regulatory compliance archive generated\n",
			time.Now().Format("15:04:05.000"))
		return nil
	}

	// 3. Register the 7 required jobs
	jobsToRegister := []struct {
		name    string
		schedFn func() (scheduler.Schedule, error)
		task    scheduler.Task
	}{
		{
			name: "heartbeat-30s",
			schedFn: func() (scheduler.Schedule, error) {
				return scheduler.Every(30 * time.Second)
			},
			task: task30s,
		},
		{
			name: "metrics-1m",
			schedFn: func() (scheduler.Schedule, error) {
				return scheduler.Every(1 * time.Minute)
			},
			task: task1m,
		},
		{
			name: "market-scan-10m",
			schedFn: func() (scheduler.Schedule, error) {
				return scheduler.Every(10 * time.Minute)
			},
			task: task10m,
		},
		{
			name: "clock-hourly",
			schedFn: func() (scheduler.Schedule, error) {
				// minute=0, second=0 ensures execution at the top of each clock hour
				return scheduler.Hourly()
			},
			task: taskHourly,
		},
		{
			name: "daily-digest-9am",
			schedFn: func() (scheduler.Schedule, error) {
				// Every day at 09:00:00 local time
				return scheduler.Daily(9, 0, 0)
			},
			task: taskDaily,
		},
		{
			name: "monthly-report-1st",
			schedFn: func() (scheduler.Schedule, error) {
				// 1st of every month at 00:00:00 local time
				return scheduler.Monthly(1, 0, 0, 0)
			},
			task: taskMonthly,
		},
		{
			name: "yearly-newyear-jan1",
			schedFn: func() (scheduler.Schedule, error) {
				// Jan 1st at 00:00:00 local time
				return scheduler.Yearly(time.January, 1, 0, 0, 0)
			},
			task: taskYearly,
		},
	}

	for _, item := range jobsToRegister {
		s, err := item.schedFn()
		if err != nil {
			log.Fatalf("Failed to construct schedule for %q: %v", item.name, err)
		}
		if err := sched.Register(item.name, s, item.task, commonOpts...); err != nil {
			log.Fatalf("Failed to register job %q: %v", item.name, err)
		}
	}

	// 4. Start the Scheduler
	if err := sched.Start(); err != nil {
		log.Fatalf("Failed to start scheduler: %v", err)
	}
	fmt.Println("[STATUS] Scheduler started successfully. Running background event loop.")
	fmt.Println()

	// 5. Display registered job roster and calculated next run times
	fmt.Printf("%-22s | %-32s | %-25s | %-12s\n", "JOB NAME", "SCHEDULE DEFINITION", "NEXT SCHEDULED EXECUTION", "TIME UNTIL")
	fmt.Println("---------------------------------------------------------------------------------------------------------------")
	for _, j := range sched.Jobs() {
		durUntil := time.Until(j.NextRun).Round(time.Second)
		fmt.Printf("%-22s | %-32s | %-25s | in %-10s\n",
			j.Name,
			j.ScheduleDesc,
			j.NextRun.Format("2006-01-02 15:04:05"),
			durUntil,
		)
	}
	fmt.Println("---------------------------------------------------------------------------------------------------------------")
	fmt.Println("Press Ctrl+C to initiate graceful shutdown.")
	fmt.Println()

	// 6. Graceful shutdown on OS signal (SIGINT / SIGTERM)
	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, os.Interrupt, syscall.SIGTERM)

	sig := <-shutdownSignal
	fmt.Printf("\n[SHUTDOWN] Received signal %v. Initiating graceful shutdown...\n", sig)

	// Allow up to 5 seconds for in-flight tasks to finish
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := sched.Stop(shutdownCtx); err != nil {
		log.Printf("[SHUTDOWN ERROR] Scheduler shutdown error: %v\n", err)
	} else {
		fmt.Println("[SHUTDOWN] All in-flight tasks completed. Scheduler stopped cleanly.")
	}
}
