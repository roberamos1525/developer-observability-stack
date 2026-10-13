package traces

import (
	"sync"
	"testing"
	"time"
)

func TestConcurrentAppendSearchAndRetention(t *testing.T) {
	s, err := Open(Options{Dir: t.TempDir(), PartitionDuration: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				if _, err := s.Append(sampleTrace(w*100+i+1, int64(i)*10, i%5 == 0)); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	stop := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			s.Search(SearchQuery{Service: "api", ErrorsOnly: true})
			s.Dependencies(0, 0)
			s.ApplyRetention(0)
		}
	}()
	wg.Wait()
	close(stop)
	readers.Wait()
	if st := s.Stats(); st.Traces != 100 || st.Spans != 400 {
		t.Fatalf("stats %+v", st)
	}
	if got := len(s.Search(SearchQuery{ErrorsOnly: true, Limit: 1000})); got != 20 {
		t.Fatalf("error traces %d", got)
	}
}
