package main

// OpType defines the operation type for a log entry
type OpType = byte

const (
	OpTypePut    OpType = 0
	OpTypeDelete OpType = 1
)

// InternalKey combines the user key with metadata for versioning
type InternalKey struct {
	UserKey string
	SeqNum  uint64
	Type    OpType
}
type internalKeyComparable struct{}

// implement to be an interface, not used
func (c internalKeyComparable) CalcScore(key interface{}) float64 {
	return 0
}

// compare sorts by UserKey ascending, then by SeqNum descending
func (c internalKeyComparable) Compare(k1, k2 interface{}) int {
	ik1 := k1.(InternalKey)
	ik2 := k2.(InternalKey)
	//first, compare by user key
	if ik1.UserKey > ik2.UserKey {
		return 1
	}
	if ik1.UserKey < ik2.UserKey {
		return -1
	}
	//if user keys are the same, the one with the higher sequence number is considered 'smaller'
	// so that it comes first in an iteration
	if ik1.SeqNum > ik2.SeqNum {
		return -1
	}
	if ik1.SeqNum < ik2.SeqNum {
		return 1
	}
	return 0
}
