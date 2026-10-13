package selfmon

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestQueueObserverAndGaugeFuncs(t *testing.T) {
	m := New()
	o := QueueObserver{M: m}
	o.Accepted("logs", 10)
	o.Rejected("logs", 2)
	o.Processed("logs", 7, time.Millisecond, nil)
	o.Processed("logs", 3, time.Millisecond, errors.New("disk full"))
	for outcome, want := range map[string]float64{"enqueued": 10, "rejected": 2, "persisted": 7, "failed": 3} {
		if got := testutil.ToFloat64(m.QueueItems.WithLabelValues("logs", outcome)); got != want {
			t.Errorf("%s = %v, want %v", outcome, got, want)
		}
	}
	if testutil.ToFloat64(m.BatchErrors.WithLabelValues("logs")) != 1 {
		t.Fatal("batch errors")
	}
	depth := 4.0
	m.RegisterGaugeFunc("queue_depth", "help", map[string]string{"queue": "x"}, func() float64 { return depth })
	n, err := testutil.GatherAndCount(m.Registry, "obs_queue_depth")
	if err != nil || n != 1 {
		t.Fatalf("gauge func registered %d, %v", n, err)
	}
	if err := testutil.GatherAndCompare(m.Registry, strings.NewReader(`
# HELP obs_queue_depth help
# TYPE obs_queue_depth gauge
obs_queue_depth{queue="x"} 4
`), "obs_queue_depth"); err != nil {
		t.Fatal(err)
	}
}
