package limiter

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber/tchannel-go/testutils"
)

func TestSerial(t *testing.T) {
	run := New(1000 /* maxRequests */, 100000 /* rps */, time.Second)
	for i := 0; i < 1000; i++ {
		assert.True(t, run.More(), "Request %v should succeed", i)
	}
	for i := 0; i < 1000; i++ {
		assert.False(t, run.More(), "Request %v should fail", i)
	}
}

func TestRateLimited(t *testing.T) {
	run := New(1000 /* maxRequests */, 100 /* rps */, time.Second)
	assert.True(t, run.More(), "First request should succeed")
	started := time.Now()
	assert.True(t, run.More(), "Second request should succeed")
	elapsed := time.Since(started)

	// Since RPS is 100, we should only execute one call every 10ms.
	// Because of timing + call overhead, we make a safe asseertion for > 5ms
	// but < 15ms.
	assert.True(t, elapsed > 5*time.Millisecond && elapsed < 15*time.Millisecond,
		"Second More elapsed is unexpected, expected 5ms < %v < 15ms", elapsed)
}

func TestParallel(t *testing.T) {
	run := New(1000 /* maxRequests */, 100000 /* rps */, time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				assert.True(t, run.More(), "First 100 requests in each goroutine should succeed, failed at %v", i)
			}
		}()
	}

	wg.Wait()
	assert.False(t, run.More(), "Requests should fail after 1000 parallel requests")
}

func TestStop(t *testing.T) {
	run := New(0 /* maxRequests */, 0 /* rps */, 0 /* maxDuration */)

	for i := 0; i < 100; i++ {
		assert.True(t, run.More(), "Before Stop() should succeed, iteration %v", i)
	}
	run.Stop()
	for i := 0; i < 1000; i++ {
		assert.False(t, run.More(), "After Stop() should fail")
	}

	// Make sure we can stop multiple times
	run.Stop()
	run.Stop()
	assert.False(t, run.More(), "After Stop() should fail")
}

func TestTimeout(t *testing.T) {
	run := New(1000 /* maxRequests */, 1000 /* rps */, time.Millisecond)
	assert.True(t, run.More(), "Succeed within the timeout")
	time.Sleep(5 * time.Millisecond)
	assert.False(t, run.More(), "Fail after the timeout")
}

func TestUnlimitedRequests(t *testing.T) {
	timeout := testutils.Timeout(100 * time.Millisecond)
	run := New(0 /* maxRequests */, 1000 /* rps */, timeout)
	for i := 0; i < 5; i++ {
		assert.True(t, run.More(), "Unlimited should suceed till timeout")
	}
	time.Sleep(timeout)
	assert.False(t, run.More(), "Fail after the timeout")
}

func TestUnlimitedStop(t *testing.T) {
	run := New(0 /* maxRequests */, 0 /* rps */, 0 /* maxDuration */)
	for i := 0; i < 5; i++ {
		assert.True(t, run.More(), "Unlimited should suceed till Stop")
	}
	run.Stop()
	time.Sleep(5 * time.Millisecond)
	assert.False(t, run.More(), "Fail after the timeout")
}
