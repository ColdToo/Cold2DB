package db

type Cold2 interface {
	get(key []byte) (val []byte, err error)
}
