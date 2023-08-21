package transportHttp

import (
	"encoding/binary"
	"errors"
	"github.com/ColdToo/Cold2DB/pb"
	"io"

	"go.etcd.io/etcd/pkg/pbutil"
)

type encoder interface {
	// encode encodes the given message to an output stream.
	encode(m *pb.Message) error
}

type decoder interface {
	// decode decodes the message from an input stream.
	decode() (pb.Message, error)
}

type messageEncoderAndWriter struct {
	w io.Writer
}

func (enc *messageEncoderAndWriter) encode(m *pb.Message) error {
	if err := binary.Write(enc.w, binary.BigEndian, uint64(m.Size())); err != nil {
		return err
	}
	_, err := enc.w.Write(pbutil.MustMarshal(m))
	return err
}

type messageDecoder struct {
	r io.Reader
}

var (
	readBytesLimit     uint64 = 512 * 1024 * 1024 // 512 MB
	ErrExceedSizeLimit        = errors.New("transportHttp: error limit exceeded")
)

func (dec *messageDecoder) decode() (pb.Message, error) {
	return dec.decodeLimit(readBytesLimit)
}

func (dec *messageDecoder) decodeLimit(numBytes uint64) (pb.Message, error) {
	var m pb.Message
	var l uint64
	if err := binary.Read(dec.r, binary.BigEndian, &l); err != nil {
		return m, err
	}
	if l > numBytes {
		return m, ErrExceedSizeLimit
	}
	buf := make([]byte, int(l))
	if _, err := io.ReadFull(dec.r, buf); err != nil {
		return m, err
	}
	return m, m.Unmarshal(buf)
}
