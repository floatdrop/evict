package tinylfu

import (
	"math"
	"testing"
)

// near reports whether two numbers are the same but for rounding.
func near(a, b float64) bool { return math.Abs(a-b) <= 1e-9*max(1, math.Abs(b)) }

func TestClimberInit(t *testing.T) {
	t.Parallel()

	// The small queue starts with a hundredth of the room, or with one
	// entry, and stays between that and four fifths. The period is that of
	// the whole cache, which may be in several shards.
	for _, tt := range []struct {
		capacity, total    int
		size               int
		least, most, first float64
		period             float64
	}{
		{1, 1, 1, 1, 1, 0.0625, 40},
		{2, 2, 1, 1, 1.6, 0.125, 80},
		{100, 100, 1, 1, 80, 6.25, 4000},
		{250, 250, 2, 2.5, 200, 15.625, 10_000},
		{1000, 1000, 10, 10, 800, 62.5, 40_000},
		{1000, 7999, 10, 10, 800, 62.5, 319_960},
	} {
		var c climber
		c.rate, c.measured = 0.5, true // Whatever was there before is gone.
		c.init(tt.capacity, tt.total)
		if c.size() != tt.size || !near(c.target, tt.least) || !near(c.least, tt.least) || !near(c.most, tt.most) ||
			!near(c.first, tt.first) || !near(c.step, tt.first) || c.period != tt.period || c.measured || c.rate != 0 {
			t.Errorf("capacity %d of %d: %+v, want %+v", tt.capacity, tt.total, c, tt)
		}
		// Until something has been measured, a period's worth of requests
		// is a period's worth of misses.
		if got := c.due(); got != uint64(tt.period) {
			t.Errorf("capacity %d of %d: the first move is due after %d misses, want %v", tt.capacity, tt.total, got, tt.period)
		}
	}
}

// feed has the climber move after a period of requests, of which the given
// share were answered, and returns the misses after which it moves next.
func feed(c *climber, rate float64) uint64 {
	hits := uint64(math.Round(rate * c.period))
	return c.move(hits, uint64(c.period)-hits)
}

// TestClimberMoves follows the size of the small queue through the answers
// that a cache might give.
func TestClimberMoves(t *testing.T) {
	t.Parallel()

	var c climber
	c.init(1000, 1000)

	for i, tt := range []struct {
		rate         float64
		target, step float64 // after the move
	}{
		// The first measurement is what the next one is compared with.
		// The size moves as if nothing had changed: up, the way it was
		// set to go, with a step that is shorter next time.
		{0.50, 72.5, 61.25},
		// Better: on, in steps that get shorter.
		{0.52, 133.75, 60.025},
		// Worse: back.
		{0.51, 73.725, -58.8245},
		// Better, so on in the direction of the last step, which is down.
		{0.53, 14.9005, -57.64801},
		// No less than a hundredth of the room.
		{0.54, 10, -56.4950498},
		// Much worse: back, and the steps are long again.
		{0.40, 66.4950498, 62.5},
		// Much better: on, with long steps.
		{0.50, 128.9950498, 62.5},
		// The same counts as better.
		{0.50, 191.4950498, 61.25},
	} {
		due := feed(&c, tt.rate)
		if !near(c.target, tt.target) || !near(c.step, tt.step) || c.size() != int(tt.target) || !near(c.rate, tt.rate) || !c.measured {
			t.Fatalf("move %d, to a rate of %v: target %v, size %d, next step %v, rate %v; want target %v, size %d, next step %v",
				i, tt.rate, c.target, c.size(), c.step, c.rate, tt.target, int(tt.target), tt.step)
		}
		// The next move is due after the misses that a period brings at
		// the rate that was just measured.
		if want := 40_000 * (1 - tt.rate); math.Abs(float64(due)-want) > 1 {
			t.Fatalf("move %d, to a rate of %v: the next move is due after %d misses, want %v", i, tt.rate, due, want)
		}
	}

	// As long as things get better it goes on, up to four fifths of the
	// room and no further.
	rate := 0.5
	for moves := 0; c.size() != 800; moves++ {
		if moves == 100 {
			t.Fatalf("after 100 steps up the size is %d, want 800", c.size())
		}
		last := c.size()
		rate += 0.001
		feed(&c, rate)
		if c.size() <= last || c.size() > 800 {
			t.Fatalf("a step up took the size from %d to %d", last, c.size())
		}
	}
	feed(&c, rate+0.001)
	if c.size() != 800 || c.target != 800 {
		t.Errorf("at the upper bound a step up leads to %d (%v), want 800", c.size(), c.target)
	}
}

// TestClimberRestart checks where long steps begin again: at a change of five
// points in the share of requests answered, and not below.
func TestClimberRestart(t *testing.T) {
	t.Parallel()

	var c climber
	c.init(1000, 1000)
	feed(&c, 0.50) // What the next one is compared with: the step shrinks.
	if !near(c.step, 61.25) {
		t.Fatalf("after the first measurement the next step is %v, want 61.25", c.step)
	}
	feed(&c, 0.55) // Five points better: long again.
	if !near(c.step, 62.5) {
		t.Fatalf("after a gain of five points the next step is %v, want 62.5", c.step)
	}
	feed(&c, 0.5999) // A hair less than five points: shorter.
	if !near(c.step, 61.25) {
		t.Fatalf("after a gain of 4.99 points the next step is %v, want 61.25", c.step)
	}
	feed(&c, 0.5499) // Five points worse, all but a hair: back, and shorter.
	if !near(c.step, -60.025) {
		t.Fatalf("after a loss of 5.00 points less a hair the next step is %v, want -60.025", c.step)
	}
	feed(&c, 0.4999) // Five points worse: back, and long again.
	if !near(c.step, 62.5) {
		t.Fatalf("after a loss of five points the next step is %v, want 62.5", c.step)
	}
}

// TestClimberForget checks that a climber that is told to start over takes
// the next measurement for what to compare with, and keeps its size, its
// step, and what it knows of the share of requests that misses make up.
func TestClimberForget(t *testing.T) {
	t.Parallel()

	var c climber
	c.init(1000, 1000)
	feed(&c, 0.5)
	feed(&c, 0.9) // Much better: on, with a long step.
	target, step := c.target, c.step
	c.forget()
	if c.measured || c.target != target || c.step != step {
		t.Fatalf("after forget: %+v, want the target %v and the step %v, and nothing measured", c, target, step)
	}
	if got := c.due(); math.Abs(float64(got)-4000) > 1 {
		t.Errorf("after forget the next move is due after %d misses, want 4000 as before", got)
	}
	// A much worse share of requests is not taken for a change for the
	// worse: it is the first measurement since.
	feed(&c, 0.3)
	if !near(c.target, target+step) || !near(c.step, decay*step) {
		t.Errorf("after forget and a share of 0.3 the target is %v and the step %v, want %v and %v",
			c.target, c.step, target+step, decay*step)
	}
}

// TestClimberDue checks the bounds of the time between moves.
func TestClimberDue(t *testing.T) {
	t.Parallel()

	var c climber
	c.init(1000, 1000)
	// A cache that answers nearly everything moves at the next miss.
	c.move(1_000_000_000, 1)
	if got := c.due(); got != 1 {
		t.Errorf("at a rate of %v the next move is due after %d misses, want 1", c.rate, got)
	}
	// One that answers nothing waits for a whole period of misses.
	c.move(0, 5)
	if got := c.due(); got != 40_000 {
		t.Errorf("at a rate of 0 the next move is due after %d misses, want 40,000", got)
	}
}
