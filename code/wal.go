package code

import "errors"

var ErrValueTooLarge = errors.New("the data size can't larger than segment size")
var ErrPendingSizeTooLarge = errors.New("the upper bound of pendingWrites can't larger than segment size")
var ErrClosed = errors.New("the segment file is closed")
var ErrInvalidCRC = errors.New("invalid crc, the data may be corrupted")
