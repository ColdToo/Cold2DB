package transportTCP

import (
	"fmt"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"io"
)

type messageEncoderAndWriter struct {
	w io.Writer
}

func (enc *messageEncoderAndWriter) encodeAndWrite(m *pb.Message) error {
	message, err := enc.setMessage(m)
	if err != nil {
		return err
	}
	_, err = enc.w.Write(message)
	if err != nil {
		return err
	}
	return nil
}

func (enc *messageEncoderAndWriter) setMessage(m *pb.Message) (b []byte, err error) {
	b, _ = m.Marshal()
	dp := NewDataPack()
	b, _ = dp.Pack(NewMsgPackage(0, b))
	return
}

type messageDecoder struct {
	r io.Reader
}

func (dec *messageDecoder) decodeAndRead() (pb.Message, error) {
	var m pb.Message
	msg := dec.getMessage(dec.r)
	dec.getMessage(dec.r)
	return m, m.Unmarshal(msg.GetData())
}

func (dec *messageDecoder) getMessage(rc io.Reader) (msg IMessage) {
	var err error
	dp := NewDataPack()
	headData := make([]byte, dp.GetHeadLen())
	if _, err := io.ReadFull(rc, headData); err != nil {
		log.Debugf("read msg head error ", err)
	}

	msg, err = dp.Unpack(headData)
	if err != nil {
		log.Debugf("unpack error ", err)
	}

	//根据 dataLen 读取 data，放在msg.Data中
	var data []byte
	if msg.GetDataLen() > 0 {
		data = make([]byte, msg.GetDataLen())
		if _, err := io.ReadFull(rc, data); err != nil {
			fmt.Println("read msg data error ", err)
		}
	}
	msg.SetData(data)
	return
}
