package storage

import (
	"sync"
	"time"
)

type OperationType string

const (
	SetOperation OperationType = "SET"
	DelOperation OperationType = "DEL"
)

type Operation struct {
	Sequence uint64
	Type     OperationType
	Key      string
	Value    []byte
	Index    uint64
	Time     time.Time
}

type LogStore interface {
	Append(opType OperationType, key string, value []byte) uint64

	GetAfter(sequence uint64) []Operation
}

type OperationLog struct {
	mu         sync.RWMutex
	sequence   uint64
	operations []Operation
}

func NewOperationLog() *OperationLog {
	return &OperationLog{}
}

// Append logs a new database mutation and increments the local sequence counter.
func (o *OperationLog) Append(opType OperationType, key string, value []byte) uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.sequence++
	o.operations = append(o.operations, Operation{
		Sequence: o.sequence,
		Index:    o.sequence,
		Type:     opType,
		Key:      key,
		Value:    value,
		Time:     time.Now(),
	})
	return o.sequence
}

// GetAfter returns all logged operations after a given sequence threshold.
func (o *OperationLog) GetAfter(sequence uint64) []Operation {
	o.mu.RLock()
	defer o.mu.RUnlock()

	result := []Operation{}
	for _, op := range o.operations {
		if op.Sequence > sequence {
			result = append(result, op)
		}
	}
	return result
}

// LogManager acts as an architectural wrapper/facade implementing LogStore.
type LogManager struct {
	opLog *OperationLog
}

// NewLogManager constructs a new LogManager.
func NewLogManager() *LogManager {
	return &LogManager{
		opLog: NewOperationLog(),
	}
}

// Append forwards the write to the underlying OperationLog.
func (lm *LogManager) Append(opType OperationType, key string, value []byte) uint64 {
	return lm.opLog.Append(opType, key, value)
}

// GetAfter fetches operations from the underlying OperationLog.
func (lm *LogManager) GetAfter(sequence uint64) []Operation {
	return lm.opLog.GetAfter(sequence)
}
