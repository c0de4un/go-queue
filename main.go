package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type waiter struct {
	ch      chan string
	expired bool
}

type queue struct {
	messages []string
	waiters  []*waiter
	mu       sync.Mutex
}

type broker struct {
	queues map[string]*queue
	mu     sync.Mutex
}

func newBroker() *broker {
	return &broker{queues: make(map[string]*queue)}
}

func (b *broker) getQueue(name string) *queue {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.queues[name] == nil {
		b.queues[name] = &queue{}
	}
	return b.queues[name]
}

func (q *queue) compact() {
	n := 0
	for _, w := range q.waiters {
		if !w.expired {
			q.waiters[n] = w
			n++
		}
	}
	q.waiters = q.waiters[:n]
}

func (q *queue) put(msg string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.compact()
	if len(q.waiters) > 0 {
		w := q.waiters[0]
		q.waiters = q.waiters[1:]
		q.mu.Unlock()
		w.ch <- msg // отправляем вне лока: канал буферизован, получатель уже ждёт
		return
	}
	q.messages = append(q.messages, msg)
}

func (q *queue) get(timeout *time.Duration) (string, bool) {
	q.mu.Lock()
	if len(q.messages) > 0 {
		msg := q.messages[0]
		q.messages = q.messages[1:]
		q.mu.Unlock()
		return msg, true
	}
	if timeout == nil {
		q.mu.Unlock()
		return "", false
	}

	w := &waiter{ch: make(chan string, 1)}
	q.waiters = append(q.waiters, w)
	q.mu.Unlock()

	timer := time.NewTimer(*timeout)
	defer timer.Stop()

	select {
	case msg := <-w.ch:
		return msg, true
	case <-timer.C:
		q.mu.Lock()
		// Защита от гонки: сообщение могло прийти между timer.C и Lock.
		select {
		case msg := <-w.ch:
			q.mu.Unlock()
			return msg, true
		default:
			w.expired = true
			q.compact()
			q.mu.Unlock()
			return "", false
		}
	}
}

func (b *broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Path
	if len(name) > 0 && name[0] == '/' {
		name = name[1:]
	}
	if name == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	q := b.getQueue(name)

	switch r.Method {
	case http.MethodPut:
		v := r.URL.Query().Get("v")
		if v == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		q.put(v)

	case http.MethodGet:
		var timeout *time.Duration
		if t := r.URL.Query().Get("timeout"); t != "" {
			n, err := strconv.Atoi(t)
			if err != nil || n < 0 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if n > 0 {
				d := time.Duration(n) * time.Second
				timeout = &d
			}
		}
		msg, ok := q.get(timeout)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Write([]byte(msg))

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func main() {
	port := "8080"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}
	b := newBroker()
	if err := http.ListenAndServe(":"+port, b); err != nil {
		log.Fatal(err)
	}
}
