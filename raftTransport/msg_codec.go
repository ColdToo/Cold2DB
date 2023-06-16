package raftTransport

import (
	"encoding/binary"
	"errors"
	"github.com/ColdToo/Cold2DB/raftproto"
	"io"

	"go.etcd.io/etcd/pkg/pbutil"
)

type encoder interface {
	// encode encodes the given message to an output stream.
	encode(m *raftproto.Message) error
}

type decoder interface {
	// decode decodes the message from an input stream.
	decode() (raftproto.Message, error)
}

type messageEncoderAndWriter struct {
	w io.Writer
}

func (enc *messageEncoderAndWriter) encode(m *raftproto.Message) error {
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
	ErrExceedSizeLimit        = errors.New("raftTransport: error limit exceeded")
)

func (dec *messageDecoder) decode() (raftproto.Message, error) {
	return dec.decodeLimit(readBytesLimit)
}

func (dec *messageDecoder) decodeLimit(numBytes uint64) (raftproto.Message, error) {
	var m raftproto.Message
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
