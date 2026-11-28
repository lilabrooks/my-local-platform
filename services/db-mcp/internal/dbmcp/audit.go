package dbmcp

import (
	"encoding/json"
	"io"
	"sync"
	"time"
)

const AuditWriteTimeout = 250 * time.Millisecond
const auditCapacity = 32

// AuditSink reserves space before dispatch and acknowledges complete JSON lines.
// An io.Writer may block forever. Only the single drain goroutine calls Write.
type AuditSink struct {
	mu       sync.Mutex
	writer   io.Writer
	reserved int
	queue    []*auditJob
	running  bool
	withheld int
}

type auditDecision int

const (
	pending auditDecision = iota
	acknowledged
	withheld
)

type auditJob struct {
	attempt         []byte
	terminal        []byte
	decision        auditDecision
	deadline        time.Time
	done            chan struct{}
	offset          int
	writingTerminal bool
}

// AuditSlot belongs to one invocation until it is recorded or released.
type AuditSlot struct{ sink *AuditSink }

func NewAuditSink(writer io.Writer) *AuditSink { return &AuditSink{writer: writer} }

// Reserve refuses work while a previous audit failure is outstanding. If a
// write failed outright, new demand retries its terminal record without
// admitting the caller or starting another blocked writer.
func (s *AuditSink) Reserve() *AuditSlot {
	if s == nil || s.writer == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.withheld > 0 {
		s.startLocked()
		return nil
	}
	if s.reserved == auditCapacity {
		return nil
	}
	s.reserved++
	return &AuditSlot{sink: s}
}

// Release returns unused capacity (for example when credential lookup fails).
func (p *AuditSlot) Release() {
	if p == nil || p.sink == nil {
		return
	}
	s := p.sink
	s.mu.Lock()
	s.reserved--
	p.sink = nil
	s.mu.Unlock()
}

// Record acknowledges the attempted outcome only after the complete line is
// written. On failure terminal describes the response actually sent. A late
// successful write is followed by that correction under the same request ID.
func (p *AuditSlot) Record(attempt, terminal any) bool {
	if p == nil || p.sink == nil {
		return false
	}
	first, err1 := json.Marshal(attempt)
	last, err2 := json.Marshal(terminal)
	if err1 != nil || err2 != nil {
		p.Release()
		return false
	}
	job := &auditJob{
		attempt: append(first, '\n'), terminal: append(last, '\n'), done: make(chan struct{}),
		deadline: time.Now().Add(AuditWriteTimeout),
	}
	s := p.sink
	p.sink = nil
	s.mu.Lock()
	s.queue = append(s.queue, job)
	if s.withheld > 0 {
		s.withholdLocked(job)
	}
	s.startLocked()
	s.mu.Unlock()

	timer := time.NewTimer(time.Until(job.deadline))
	defer timer.Stop()
	select {
	case <-job.done:
	case <-timer.C:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if job.decision == pending {
		s.withholdLocked(job)
	}
	return job.decision == acknowledged
}

func (s *AuditSink) withholdLocked(job *auditJob) {
	if job.decision == pending {
		job.decision = withheld
		s.withheld++
		close(job.done)
	}
}

func (s *AuditSink) startLocked() {
	if !s.running && len(s.queue) > 0 {
		s.running = true
		go s.drain()
	}
}

func (s *AuditSink) drain() {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer func() { s.running = false }()
	for len(s.queue) > 0 {
		job := s.queue[0]
		if !time.Now().Before(job.deadline) {
			s.withholdLocked(job)
		}
		if job.decision == withheld && job.offset == 0 {
			job.writingTerminal = true
		}
		line := job.attempt
		if job.writingTerminal {
			line = job.terminal
		}
		// Finish a partially emitted JSON line before switching to the
		// terminal record. Replaying its prefix would corrupt the stream.
		remaining := line[job.offset:]
		s.mu.Unlock()
		n, err := s.writer.Write(remaining)
		s.mu.Lock()
		if n >= 0 && n <= len(remaining) {
			job.offset += n
		}
		if job.offset == len(line) {
			job.offset = 0
		}
		if err != nil || n != len(remaining) {
			for _, queued := range s.queue {
				s.withholdLocked(queued)
			}
			return
		}
		if job.writingTerminal {
			s.withheld--
		} else if job.decision == pending && time.Now().Before(job.deadline) {
			job.decision = acknowledged
			close(job.done)
		} else {
			// The caller timed out while Write was blocked. Keep its slot
			// until the terminal outcome has its own complete line.
			s.withholdLocked(job)
			continue
		}
		s.queue[0] = nil
		s.queue = s.queue[1:]
		s.reserved--
	}
}

func AuditUnavailable(id string) json.RawMessage {
	body, _ := json.Marshal(document{"ok": false, "code": "unavailable", "request_id": id, "message": "Required audit is unavailable."})
	return body
}
