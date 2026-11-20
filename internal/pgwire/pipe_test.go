package pgwire

import (
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// bufPipe is net.Pipe with a buffer each way, as a socket has: a TLS client
// that refuses a certificate writes its alert while the server is still
// writing, and two unbuffered writers would wait on each other forever.
func bufPipe() (net.Conn, net.Conn) {
	ab, ba := newQueue(), newQueue()
	return &pipeEnd{r: ba, w: ab}, &pipeEnd{r: ab, w: ba}
}

type queue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	buf    []byte
	closed bool
}

func newQueue() *queue {
	q := &queue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

type pipeEnd struct {
	r, w     *queue
	mu       sync.Mutex
	deadline time.Time
	timer    *time.Timer
}

func (p *pipeEnd) Read(b []byte) (int, error) {
	q := p.r
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.buf) == 0 {
		if q.closed {
			return 0, io.EOF
		}
		if p.expired() {
			return 0, os.ErrDeadlineExceeded
		}
		q.cond.Wait()
	}
	n := copy(b, q.buf)
	q.buf = q.buf[n:]
	return n, nil
}

func (p *pipeEnd) Write(b []byte) (int, error) {
	q := p.w
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return 0, io.ErrClosedPipe
	}
	if p.expired() {
		return 0, os.ErrDeadlineExceeded
	}
	q.buf = append(q.buf, b...)
	q.cond.Broadcast()
	return len(b), nil
}

func (p *pipeEnd) expired() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.deadline.IsZero() && !time.Now().Before(p.deadline)
}

func (p *pipeEnd) Close() error {
	for _, q := range []*queue{p.r, p.w} {
		q.mu.Lock()
		q.closed = true
		q.cond.Broadcast()
		q.mu.Unlock()
	}
	return nil
}

func (p *pipeEnd) SetDeadline(t time.Time) error {
	p.mu.Lock()
	p.deadline = t
	if p.timer != nil {
		p.timer.Stop()
	}
	if !t.IsZero() {
		p.timer = time.AfterFunc(time.Until(t), func() {
			p.r.mu.Lock()
			p.r.cond.Broadcast()
			p.r.mu.Unlock()
		})
	}
	p.mu.Unlock()
	return nil
}

func (p *pipeEnd) SetReadDeadline(t time.Time) error  { return p.SetDeadline(t) }
func (p *pipeEnd) SetWriteDeadline(t time.Time) error { return p.SetDeadline(t) }
func (p *pipeEnd) LocalAddr() net.Addr                { return pipeAddr{} }
func (p *pipeEnd) RemoteAddr() net.Addr               { return pipeAddr{} }

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }
