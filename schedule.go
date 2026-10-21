package scheduler

import (
	"fmt"
	"time"
)

// Schedule defines the contract for calculating execution times for scheduled jobs.
type Schedule interface {
	// Next returns the next scheduled time strictly after the reference time 'from'.
	Next(from time.Time) time.Time

	// Description returns a human-readable explanation of the schedule.
	Description() string
}

// resolveLocation returns the first provided location, or time.Local if none is given or if loc is nil.
func resolveLocation(loc []*time.Location) *time.Location {
	if len(loc) > 0 && loc[0] != nil {
		return loc[0]
	}
	return time.Local
}

// daysInMonth returns the total number of days in the specified year and month.
func daysInMonth(year int, month time.Month, loc *time.Location) int {
	// Day 0 of month+1 is the last day of month.
	return time.Date(year, month+1, 0, 0, 0, 0, 0, loc).Day()
}

// --- Interval Schedule ---

type intervalSchedule struct {
	interval time.Duration
}

// Every returns a Schedule that triggers repeatedly after the specified duration interval.
// The interval must be greater than zero.
func Every(interval time.Duration) (Schedule, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("%w: got %v", ErrInvalidInterval, interval)
	}
	return &intervalSchedule{interval: interval}, nil
}

func (s *intervalSchedule) Next(from time.Time) time.Time {
	return from.Add(s.interval)
}

func (s *intervalSchedule) Description() string {
	return fmt.Sprintf("Every %s", s.interval)
}

// --- Hourly Schedule ---

type hourlySchedule struct {
	minute   int
	second   int
	location *time.Location
}

// Hourly returns a Schedule that triggers at the beginning of every clock hour (minute 0, second 0).
// If a location is provided, calculations use that location; otherwise time.Local is used.
func Hourly(loc ...*time.Location) (Schedule, error) {
	return HourlyAt(0, 0, loc...)
}

// HourlyAt returns a Schedule that triggers at the specified minute and second of every clock hour.
func HourlyAt(minute, second int, loc ...*time.Location) (Schedule, error) {
	if minute < 0 || minute > 59 {
		return nil, fmt.Errorf("%w: minute must be between 0 and 59, got %d", ErrInvalidSchedule, minute)
	}
	if second < 0 || second > 59 {
		return nil, fmt.Errorf("%w: second must be between 0 and 59, got %d", ErrInvalidSchedule, second)
	}
	return &hourlySchedule{
		minute:   minute,
		second:   second,
		location: resolveLocation(loc),
	}, nil
}

func (s *hourlySchedule) Next(from time.Time) time.Time {
	fromInLoc := from.In(s.location)
	candidate := time.Date(
		fromInLoc.Year(),
		fromInLoc.Month(),
		fromInLoc.Day(),
		fromInLoc.Hour(),
		s.minute,
		s.second,
		0,
		s.location,
	)

	if candidate.After(fromInLoc) {
		return candidate
	}

	// Advance by 1 hour, re-normalizing through time.Date to preserve exact minute/second and handle DST.
	nextHour := candidate.Add(1 * time.Hour)
	return time.Date(
		nextHour.Year(),
		nextHour.Month(),
		nextHour.Day(),
		nextHour.Hour(),
		s.minute,
		s.second,
		0,
		s.location,
	)
}

func (s *hourlySchedule) Description() string {
	return fmt.Sprintf("Hourly at %02d:%02d (%s)", s.minute, s.second, s.location)
}

// --- Daily Schedule ---

type dailySchedule struct {
	hour     int
	minute   int
	second   int
	location *time.Location
}

// Daily returns a Schedule that triggers every day at the specified hour, minute, and second.
// Hours are specified in 24-hour format (0-23).
func Daily(hour, minute, second int, loc ...*time.Location) (Schedule, error) {
	if hour < 0 || hour > 23 {
		return nil, fmt.Errorf("%w: hour must be between 0 and 23, got %d", ErrInvalidSchedule, hour)
	}
	if minute < 0 || minute > 59 {
		return nil, fmt.Errorf("%w: minute must be between 0 and 59, got %d", ErrInvalidSchedule, minute)
	}
	if second < 0 || second > 59 {
		return nil, fmt.Errorf("%w: second must be between 0 and 59, got %d", ErrInvalidSchedule, second)
	}

	return &dailySchedule{
		hour:     hour,
		minute:   minute,
		second:   second,
		location: resolveLocation(loc),
	}, nil
}

func (s *dailySchedule) Next(from time.Time) time.Time {
	fromInLoc := from.In(s.location)
	candidate := time.Date(
		fromInLoc.Year(),
		fromInLoc.Month(),
		fromInLoc.Day(),
		s.hour,
		s.minute,
		s.second,
		0,
		s.location,
	)

	if candidate.After(fromInLoc) {
		return candidate
	}

	// Advance to the next calendar day.
	tomorrow := fromInLoc.AddDate(0, 0, 1)
	return time.Date(
		tomorrow.Year(),
		tomorrow.Month(),
		tomorrow.Day(),
		s.hour,
		s.minute,
		s.second,
		0,
		s.location,
	)
}

func (s *dailySchedule) Description() string {
	return fmt.Sprintf("Daily at %02d:%02d:%02d (%s)", s.hour, s.minute, s.second, s.location)
}

// --- Monthly Schedule ---

type monthlySchedule struct {
	day      int
	hour     int
	minute   int
	second   int
	location *time.Location
}

// Monthly returns a Schedule that triggers once every month on the specified day, hour, minute, and second.
// If a given month contains fewer days than 'day' (for example, February 31st), the schedule automatically
// clamps execution to the final valid day of that month (e.g. February 28 or 29, April 30) at the specified time.
func Monthly(day, hour, minute, second int, loc ...*time.Location) (Schedule, error) {
	if day < 1 || day > 31 {
		return nil, fmt.Errorf("%w: day must be between 1 and 31, got %d", ErrInvalidSchedule, day)
	}
	if hour < 0 || hour > 23 {
		return nil, fmt.Errorf("%w: hour must be between 0 and 23, got %d", ErrInvalidSchedule, hour)
	}
	if minute < 0 || minute > 59 {
		return nil, fmt.Errorf("%w: minute must be between 0 and 59, got %d", ErrInvalidSchedule, minute)
	}
	if second < 0 || second > 59 {
		return nil, fmt.Errorf("%w: second must be between 0 and 59, got %d", ErrInvalidSchedule, second)
	}

	return &monthlySchedule{
		day:      day,
		hour:     hour,
		minute:   minute,
		second:   second,
		location: resolveLocation(loc),
	}, nil
}

func (s *monthlySchedule) Next(from time.Time) time.Time {
	fromInLoc := from.In(s.location)
	year, month, _ := fromInLoc.Date()

	// Check if this month's execution is still in the future.
	dim := daysInMonth(year, month, s.location)
	effectiveDay := s.day
	if effectiveDay > dim {
		effectiveDay = dim
	}

	candidate := time.Date(year, month, effectiveDay, s.hour, s.minute, s.second, 0, s.location)
	if candidate.After(fromInLoc) {
		return candidate
	}

	// Move forward to subsequent months until we find the next future occurrence.
	for {
		month++
		if month > 12 {
			month = 1
			year++
		}
		dim = daysInMonth(year, month, s.location)
		effectiveDay = s.day
		if effectiveDay > dim {
			effectiveDay = dim
		}
		candidate = time.Date(year, month, effectiveDay, s.hour, s.minute, s.second, 0, s.location)
		if candidate.After(fromInLoc) {
			return candidate
		}
	}
}

func (s *monthlySchedule) Description() string {
	return fmt.Sprintf("Monthly on day %d at %02d:%02d:%02d (%s)", s.day, s.hour, s.minute, s.second, s.location)
}

// --- Yearly Schedule ---

type yearlySchedule struct {
	month    time.Month
	day      int
	hour     int
	minute   int
	second   int
	location *time.Location
}

// Yearly returns a Schedule that triggers once every year on the specified month, day, hour, minute, and second.
// For February 29 on non-leap years, it automatically clamps to February 28 so that yearly execution is never skipped.
func Yearly(month time.Month, day, hour, minute, second int, loc ...*time.Location) (Schedule, error) {
	if month < 1 || month > 12 {
		return nil, fmt.Errorf("%w: month must be between 1 and 12, got %d", ErrInvalidSchedule, month)
	}
	if day < 1 || day > 31 {
		return nil, fmt.Errorf("%w: day must be between 1 and 31, got %d", ErrInvalidSchedule, day)
	}
	if (month == time.April || month == time.June || month == time.September || month == time.November) && day > 30 {
		return nil, fmt.Errorf("%w: %s has only 30 days, got day %d", ErrInvalidSchedule, month, day)
	}
	if month == time.February && day > 29 {
		return nil, fmt.Errorf("%w: February has at most 29 days, got day %d", ErrInvalidSchedule, day)
	}
	if hour < 0 || hour > 23 {
		return nil, fmt.Errorf("%w: hour must be between 0 and 23, got %d", ErrInvalidSchedule, hour)
	}
	if minute < 0 || minute > 59 {
		return nil, fmt.Errorf("%w: minute must be between 0 and 59, got %d", ErrInvalidSchedule, minute)
	}
	if second < 0 || second > 59 {
		return nil, fmt.Errorf("%w: second must be between 0 and 59, got %d", ErrInvalidSchedule, second)
	}

	return &yearlySchedule{
		month:    month,
		day:      day,
		hour:     hour,
		minute:   minute,
		second:   second,
		location: resolveLocation(loc),
	}, nil
}

func (s *yearlySchedule) Next(from time.Time) time.Time {
	fromInLoc := from.In(s.location)
	year := fromInLoc.Year()

	// Check if this year's occurrence is still in the future.
	dim := daysInMonth(year, s.month, s.location)
	effectiveDay := s.day
	if effectiveDay > dim {
		effectiveDay = dim
	}

	candidate := time.Date(year, s.month, effectiveDay, s.hour, s.minute, s.second, 0, s.location)
	if candidate.After(fromInLoc) {
		return candidate
	}

	// Move forward to subsequent years.
	for {
		year++
		dim = daysInMonth(year, s.month, s.location)
		effectiveDay = s.day
		if effectiveDay > dim {
			effectiveDay = dim
		}
		candidate = time.Date(year, s.month, effectiveDay, s.hour, s.minute, s.second, 0, s.location)
		if candidate.After(fromInLoc) {
			return candidate
		}
	}
}

func (s *yearlySchedule) Description() string {
	return fmt.Sprintf("Yearly on %s %d at %02d:%02d:%02d (%s)", s.month, s.day, s.hour, s.minute, s.second, s.location)
}
