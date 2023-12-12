package code

import "errors"

var ErrIllegalMemtableNums = errors.New("requested index is unavailable due to compaction")

var (
	// ErrRecordExists record with this key already exists.
	ErrRecordExists = errors.New("record with this key already exists")

	// ErrRecordExists record with this key already exists.
	ErrRecordNotExists = errors.New("record with this key not exists")

	// ErrRecordUpdated record was updated by another caller.
	ErrRecordUpdated = errors.New("record was updated by another caller")

	// ErrRecordDeleted record was deleted by another caller.
	ErrRecordDeleted = errors.New("record was deleted by another caller")

	ErrDBNotInit = errors.New("db is no init complete")
)

var ErrValueTooLarge = errors.New("the data size can't larger than segment size")
var ErrPendingSizeTooLarge = errors.New("the upper bound of pendingWrites can't larger than segment size")
var ErrClosed = errors.New("the segment file is closed")
var ErrInvalidCRC = errors.New("invalid crc, the data may be corrupted")
var ErrCanNotFondSSTFile = errors.New("can not foundd sst file")
