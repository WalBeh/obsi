package store

// RingBuf is a generic fixed-size circular buffer for time-series data.
type RingBuf[T any] struct {
	data  []T
	head  int
	count int
	cap   int
}

// NewRingBuf creates a ring buffer with the given capacity.
func NewRingBuf[T any](capacity int) *RingBuf[T] {
	return &RingBuf[T]{
		data: make([]T, capacity),
		cap:  capacity,
	}
}

// Push adds a value to the buffer, overwriting the oldest if full.
func (r *RingBuf[T]) Push(v T) {
	r.data[r.head] = v
	r.head = (r.head + 1) % r.cap
	if r.count < r.cap {
		r.count++
	}
}

// Slice returns all values oldest-to-newest.
func (r *RingBuf[T]) Slice() []T {
	if r.count == 0 {
		return nil
	}
	result := make([]T, r.count)
	if r.count < r.cap {
		copy(result, r.data[:r.count])
	} else {
		start := r.head % r.cap
		n := copy(result, r.data[start:])
		copy(result[n:], r.data[:start])
	}
	return result
}

// Len returns the number of values in the buffer.
func (r *RingBuf[T]) Len() int {
	return r.count
}

// Last returns the most recently added value.
func (r *RingBuf[T]) Last() (T, bool) {
	if r.count == 0 {
		var zero T
		return zero, false
	}
	idx := (r.head - 1 + r.cap) % r.cap
	return r.data[idx], true
}
