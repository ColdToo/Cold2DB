package domain

type KV struct {
	Id        uint64
	Key       []byte
	Val       []byte
	Type      int8  // 0 remove 1 put
	ExpiredAt int64 //
}
