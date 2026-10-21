package scheduler

import (
	"errors"
	"testing"
	"time"
)

func TestIntervalSchedule_Validation(t *testing.T) {
	_, err := Every(0)
	if !errors.Is(err, ErrInvalidInterval) {
		t.Fatalf("expected ErrInvalidInterval for 0 duration, got %v", err)
	}

	_, err = Every(-5 * time.Second)
	if !errors.Is(err, ErrInvalidInterval) {
		t.Fatalf("expected ErrInvalidInterval for negative duration, got %v", err)
	}

	sched, err := Every(10 * time.Second)
	if err != nil {
		t.Fatalf("unexpected error for valid duration: %v", err)
	}
	if sched.Description() != "Every 10s" {
		t.Errorf("unexpected description: %q", sched.Description())
	}
}

func TestIntervalSchedule_Calculation(t *testing.T) {
	sched, err := Every(30 * time.Second)
	if err != nil {
		t.Fatalf("failed to create interval schedule: %v", err)
	}

	ref := time.Date(2026, 10, 19, 10, 0, 0, 0, time.UTC)
	next := sched.Next(ref)
	expected := time.Date(2026, 10, 19, 10, 0, 30, 0, time.UTC)

	if !next.Equal(expected) {
		t.Errorf("expected %v, got %v", expected, next)
	}
}

func TestHourlySchedule_Validation(t *testing.T) {
	tests := []struct {
		name    string
		minute  int
		second  int
		wantErr bool
	}{
		{"valid top of hour", 0, 0, false},
		{"valid mid hour", 30, 45, false},
		{"invalid negative minute", -1, 0, true},
		{"invalid minute 60", 60, 0, true},
		{"invalid negative second", 0, -1, true},
		{"invalid second 60", 0, 60, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := HourlyAt(tt.minute, tt.second)
			if (err != nil) != tt.wantErr {
				t.Errorf("HourlyAt(%d, %d) error = %v, wantErr %v", tt.minute, tt.second, err, tt.wantErr)
			}
		})
	}
}

func TestHourlySchedule_ClockAlignment(t *testing.T) {
	loc := time.UTC
	sched, err := Hourly(loc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Case 1: Reference is mid-hour 09:15:30 -> Next should be 10:00:00
	ref := time.Date(2026, 10, 19, 9, 15, 30, 0, loc)
	next := sched.Next(ref)
	expected := time.Date(2026, 10, 19, 10, 0, 0, 0, loc)
	if !next.Equal(expected) {
		t.Errorf("expected %v, got %v", expected, next)
	}

	// Case 2: Reference is exactly 09:00:00 -> Next must be strictly after, so 10:00:00
	ref2 := time.Date(2026, 10, 19, 9, 0, 0, 0, loc)
	next2 := sched.Next(ref2)
	expected2 := time.Date(2026, 10, 19, 10, 0, 0, 0, loc)
	if !next2.Equal(expected2) {
		t.Errorf("expected %v, got %v", expected2, next2)
	}

	// Case 3: Midnight rollover 23:45:00 -> Next day 00:00:00
	ref3 := time.Date(2026, 10, 19, 23, 45, 0, 0, loc)
	next3 := sched.Next(ref3)
	expected3 := time.Date(2026, 10, 20, 0, 0, 0, 0, loc)
	if !next3.Equal(expected3) {
		t.Errorf("expected %v, got %v", expected3, next3)
	}
}

func TestDailySchedule_Calculation(t *testing.T) {
	loc := time.UTC
	sched, err := Daily(9, 0, 0, loc) // 09:00:00 every day
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Before 09:00:00 today -> Should trigger today at 09:00:00
	refBefore := time.Date(2026, 10, 19, 7, 30, 0, 0, loc)
	nextBefore := sched.Next(refBefore)
	expectedBefore := time.Date(2026, 10, 19, 9, 0, 0, 0, loc)
	if !nextBefore.Equal(expectedBefore) {
		t.Errorf("expected %v, got %v", expectedBefore, nextBefore)
	}

	// At 09:00:00 today -> Next must be strictly after, so tomorrow at 09:00:00
	refAt := time.Date(2026, 10, 19, 9, 0, 0, 0, loc)
	nextAt := sched.Next(refAt)
	expectedAt := time.Date(2026, 10, 20, 9, 0, 0, 0, loc)
	if !nextAt.Equal(expectedAt) {
		t.Errorf("expected %v, got %v", expectedAt, nextAt)
	}

	// After 09:00:00 today -> Tomorrow at 09:00:00
	refAfter := time.Date(2026, 10, 19, 14, 0, 0, 0, loc)
	nextAfter := sched.Next(refAfter)
	expectedAfter := time.Date(2026, 10, 20, 9, 0, 0, 0, loc)
	if !nextAfter.Equal(expectedAfter) {
		t.Errorf("expected %v, got %v", expectedAfter, nextAfter)
	}

	// Year rollover: Dec 31 after 09:00 -> Jan 1 next year at 09:00
	refNYE := time.Date(2026, 12, 31, 23, 0, 0, 0, loc)
	nextNYE := sched.Next(refNYE)
	expectedNYE := time.Date(2027, 1, 1, 9, 0, 0, 0, loc)
	if !nextNYE.Equal(expectedNYE) {
		t.Errorf("expected %v, got %v", expectedNYE, nextNYE)
	}
}

func TestMonthlySchedule_CalendarClamping(t *testing.T) {
	loc := time.UTC
	// Schedule for 31st of every month at 10:00:00
	sched, err := Monthly(31, 10, 0, 0, loc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// From Jan 31 11:00:00:
	// Next month is February 2026 (non-leap year, 28 days).
	// Must clamp to Feb 28 at 10:00:00!
	refJan := time.Date(2026, 1, 31, 11, 0, 0, 0, loc)
	nextFeb := sched.Next(refJan)
	expectedFeb := time.Date(2026, 2, 28, 10, 0, 0, 0, loc)
	if !nextFeb.Equal(expectedFeb) {
		t.Errorf("expected Feb clamping to %v, got %v", expectedFeb, nextFeb)
	}

	// From Feb 28 11:00:00:
	// Next month is March (31 days). Target is March 31 at 10:00:00.
	nextMar := sched.Next(nextFeb)
	expectedMar := time.Date(2026, 3, 31, 10, 0, 0, 0, loc)
	if !nextMar.Equal(expectedMar) {
		t.Errorf("expected March to be %v, got %v", expectedMar, nextMar)
	}

	// From March 31 11:00:00:
	// Next month is April (30 days). Must clamp to April 30 at 10:00:00!
	nextApr := sched.Next(nextMar)
	expectedApr := time.Date(2026, 4, 30, 10, 0, 0, 0, loc)
	if !nextApr.Equal(expectedApr) {
		t.Errorf("expected April clamping to %v, got %v", expectedApr, nextApr)
	}

	// Leap year test: February 2028 has 29 days
	refJan2028 := time.Date(2028, 1, 31, 11, 0, 0, 0, loc)
	nextFeb2028 := sched.Next(refJan2028)
	expectedFeb2028 := time.Date(2028, 2, 29, 10, 0, 0, 0, loc)
	if !nextFeb2028.Equal(expectedFeb2028) {
		t.Errorf("expected leap year Feb clamping to %v, got %v", expectedFeb2028, nextFeb2028)
	}
}

func TestYearlySchedule_LeapYearAndValidation(t *testing.T) {
	loc := time.UTC

	// Validation checks:
	// Feb 30 is impossible
	if _, err := Yearly(time.February, 30, 0, 0, 0, loc); !errors.Is(err, ErrInvalidSchedule) {
		t.Errorf("expected ErrInvalidSchedule for Feb 30, got %v", err)
	}
	// April 31 is impossible
	if _, err := Yearly(time.April, 31, 0, 0, 0, loc); !errors.Is(err, ErrInvalidSchedule) {
		t.Errorf("expected ErrInvalidSchedule for April 31, got %v", err)
	}
	// Feb 29 is valid (leap day exists)
	schedFeb29, err := Yearly(time.February, 29, 12, 0, 0, loc)
	if err != nil {
		t.Fatalf("unexpected error for Feb 29 schedule: %v", err)
	}

	// In 2026 (non-leap year), it clamps to Feb 28
	ref2025 := time.Date(2025, 12, 1, 0, 0, 0, 0, loc)
	next2026 := schedFeb29.Next(ref2025)
	expected2026 := time.Date(2026, 2, 28, 12, 0, 0, 0, loc)
	if !next2026.Equal(expected2026) {
		t.Errorf("expected 2026 non-leap run at %v, got %v", expected2026, next2026)
	}

	// In 2028 (leap year), it runs on Feb 29
	ref2027 := time.Date(2027, 12, 1, 0, 0, 0, 0, loc)
	next2028 := schedFeb29.Next(ref2027)
	expected2028 := time.Date(2028, 2, 29, 12, 0, 0, 0, loc)
	if !next2028.Equal(expected2028) {
		t.Errorf("expected 2028 leap year run at %v, got %v", expected2028, next2028)
	}

	// New Year schedule (Jan 1 00:00:00)
	schedNY, err := Yearly(time.January, 1, 0, 0, 0, loc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	refMidYear := time.Date(2026, 6, 15, 12, 0, 0, 0, loc)
	nextNY := schedNY.Next(refMidYear)
	expectedNY := time.Date(2027, 1, 1, 0, 0, 0, 0, loc)
	if !nextNY.Equal(expectedNY) {
		t.Errorf("expected New Year at %v, got %v", expectedNY, nextNY)
	}
}
