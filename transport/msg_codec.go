package transport

import (
	"bytes"
	"encoding/binary"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"io"
)

type messageEncoderAndWriter struct {
	w io.WriteCloser
}

func (enc *messageEncoderAndWriter) encodeAndWrite(m pb.Message) error {
	pkg, _ := enc.getPackageBin(m)
	_, err := enc.w.Write(pkg)
	if err != nil {
		return err
	}
	return nil
}

func (enc *messageEncoderAndWriter) getPackageBin(m pb.Message) (b []byte, err error) {
	b, _ = m.Marshal()
	b, _ = NewPackage(1, b).PackToBinary()
	return
}

type messageDecoderAndReader struct {
	r io.ReadCloser
}

func (dec *messageDecoderAndReader) decodeAndRead() (pb.Message, error) {
	var m pb.Message
	pkg, err := GetPack(dec.r)
	if err != nil {
		return pb.Message{}, err
	}
	return m, m.Unmarshal(pkg.Data)
}

const (
	// HeaderLength id一个字节 data长度4个字节
	HeaderLength = 5
)

type Package struct {
	DataLen uint32
	Id      uint8
	Data    []byte
}

func NewPackage(id uint8, data []byte) *Package {
	return &Package{
		DataLen: uint32(len(data)),
		Id:      id,
		Data:    data,
	}
}

func (pkg *Package) PackToBinary() ([]byte, error) {
	dataBuff := bytes.NewBuffer([]byte{})

	if err := binary.Write(dataBuff, binary.LittleEndian, pkg.DataLen); err != nil {
		return nil, err
	}

	if err := binary.Write(dataBuff, binary.LittleEndian, pkg.Id); err != nil {
		return nil, err
	}

	if err := binary.Write(dataBuff, binary.LittleEndian, pkg.Data); err != nil {
		return nil, err
	}

	return dataBuff.Bytes(), nil
}

func GetPack(rc io.Reader) (pkg Package, err error) {
	headData := make([]byte, HeaderLength)
	if _, err := io.ReadFull(rc, headData); err != nil {
		log.Debugf("read msg head error ", err)
	}
	dataBuff := bytes.NewReader(headData)
	err = binary.Read(dataBuff, binary.LittleEndian, &pkg.DataLen)
	err = binary.Read(dataBuff, binary.LittleEndian, &pkg.Id)
	if err != nil {
		log.Errorf("unpack error ", err)
		return
	}

	var data []byte
	if pkg.DataLen > 0 {
		data = make([]byte, pkg.DataLen)
		if _, err = io.ReadFull(rc, data); err != nil {
			log.Errorf("read pkg data error ", err)
			return
		}
	}
	pkg.Data = data
	return
}
