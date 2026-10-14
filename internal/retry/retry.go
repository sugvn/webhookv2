package retry

import (
	"math/rand/v2"
	"time"
)

type Policy struct {
	MaxAttempts int           // must equal the p_max_attempts passed to claim_deliveries()
	Base        time.Duration // delay ceiling after the first failure
	Cap         time.Duration // upper bound on any delay
}

func Default() Policy {
	return Policy{MaxAttempts: 5, Base: 30 * time.Second, Cap: 10 * time.Minute}
}

// Next decides what happens after a failed attempt. `attempts` is deliveries.attempts
// as returned by the claim, i.e. it already counts the attempt that just failed.
// dead == true means dead-letter; otherwise retry after delay.
func (p Policy) Next(attempts int) (delay time.Duration, dead bool) {
	return p.next(attempts, rand.Float64)
}

func (p Policy) next(attempts int, rnd func() float64) (time.Duration, bool) {
	if attempts >= p.MaxAttempts {
		return 0, true
	}
	return p.backoff(attempts, rnd), false
}

// Equal jitter: half the exponential delay is fixed, half is random.
func (p Policy) backoff(attempts int, rnd func() float64) time.Duration {
	d := p.Base
	for i := 1; i < attempts && d < p.Cap; i++ {
		d *= 2 // d < Cap before doubling, so this cannot overflow
	}
	if d > p.Cap {
		d = p.Cap
	}
	half := d / 2
	return half + time.Duration(rnd()*float64(half))
}
