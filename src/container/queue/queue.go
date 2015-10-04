package queue

import (
	"object"
)

type Queue struct {
	queues []chan object.Object
}

func NewQueue(qn, ql int) *Queue {
	qSlice := make([]chan object.Object, qn)
	for i, _ := range qSlice {
		qSlice[i] = make(chan object.Object, ql)
	}
	return &Queue{
		queues: qSlice,
	}
}
