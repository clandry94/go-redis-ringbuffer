package go_redis_ringbuffer


import (
	"github.com/go-redis/redis"
	"time"
)

type RingBuffer struct {
	client *redis.Client
	Capacity int64
	Name string
	timeout time.Duration
	size int64
}

func NewRingBuffer(name string, capacity int64, client *redis.Client) *RingBuffer {
	return &RingBuffer{
		client: client,
		Capacity: capacity,
		Name: name,
		timeout: 1,
		size: 0,
	}
}

// Insert into the ring
func (rb *RingBuffer) Push(item string) error {
	if rb.size == rb.Capacity - 1 {
		_, err := rb.Pop()
		if err != nil {
			return err
		}
	}

	err := rb.client.RPush(rb.Name, item).Err()
	if err != nil {
		rb.size++
		return nil
	}
	return err
}

// Pop the last element added to the ring
func (rb *RingBuffer) Pop() (string, error) {
	result, err := rb.client.BLPop(rb.timeout * time.Second, rb.Name).Result()
	if err != nil {
		return "", err
	}

	rb.size--
	return result[1], nil
}

func (rb *RingBuffer) GetRange(start int64, stop int64) ([]string, error) {
	result, err := rb.client.LRange(rb.Name, start, stop).Result()
	if err != nil {
		return nil, err
	}

	return result, nil
}