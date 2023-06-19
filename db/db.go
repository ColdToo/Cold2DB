package db

type Cold2Op interface {
	Get(key []byte) (val []byte, err error)
	Put(key, val []byte) (ok bool, err error)
}

type Cold2 struct {
}

func InitDB() *Cold2 {
	return new(Cold2)
}

func (db Cold2) Get(key []byte) (val []byte, err error) {
	return
}

func (db Cold2) Put(key, val []byte) (ok bool, err error) {
	return
}
