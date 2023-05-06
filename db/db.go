package db

type Cold2 interface {
	Get(key []byte) (val []byte, err error)
	Put(key, val []byte) (ok bool, err error)
}
