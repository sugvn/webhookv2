package retry

import (
	"testing"
	"time"
)

func TestNext(t *testing.T) {
	p := Policy{MaxAttempts: 5, Base: 30 * time.Second, Cap: 10 * time.Minute}
	cases := []struct {
		attempts int
		dead     bool
		lo, hi   time.Duration // delay bounds at rnd=0 and rnd→1
	}{
		{1, false, 15 * time.Second, 30 * time.Second},
		{2, false, 30 * time.Second, 60 * time.Second},
		{3, false, 60 * time.Second, 120 * time.Second},
		{4, false, 120 * time.Second, 240 * time.Second},
		{5, true, 0, 0},
		{6, true, 0, 0}, // defensive: past the cap is still dead
	}
	for _, c := range cases {
		d0, dead0 := p.next(c.attempts, func() float64 { return 0 })
		d1, dead1 := p.next(c.attempts, func() float64 { return 0.999999 })
		if dead0 != c.dead || dead1 != c.dead {
			t.Fatalf("attempts=%d: dead=%v/%v want %v", c.attempts, dead0, dead1, c.dead)
		}
		if c.dead {
			continue
		}
		if d0 != c.lo {
			t.Errorf("attempts=%d: min delay %v want %v", c.attempts, d0, c.lo)
		}
		if d1 < c.lo || d1 > c.hi {
			t.Errorf("attempts=%d: max delay %v outside [%v,%v]", c.attempts, d1, c.lo, c.hi)
		}
	}
}

func TestCap(t *testing.T) {
	p := Policy{MaxAttempts: 100, Base: 30 * time.Second, Cap: 10 * time.Minute}
	for a := 1; a < 100; a++ {
		d, _ := p.next(a, func() float64 { return 0.999999 })
		if d > p.Cap {
			t.Fatalf("attempts=%d: delay %v exceeds cap %v", a, d, p.Cap)
		}
	}
}
