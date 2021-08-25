package mast

import (
	"encoding/binary"
	"errors"
	"fmt"
)

func appendLength(buf []byte, n int) []byte {
	var tmpbuf [8]byte
	len := binary.PutUvarint(tmpbuf[:], uint64(n))
	return append(buf, tmpbuf[:len]...)
}

func appendEfaceSlice[T any](buf []byte, l []T, marshal func(interface{}) ([]byte, error)) ([]byte, error) {
	buf = appendLength(buf, len(l))
	for _, elem := range l {
		body, err := marshal(elem)
		if err != nil {
			return nil, err
		}
		buf = appendLength(buf, len(body))
		buf = append(buf, body...)
	}
	return buf, nil
}

func decodeLength(buf []byte, n *int) ([]byte, error) {
	k, len := binary.Uvarint(buf)
	if len <= 0 {
		return nil, errors.New("bad length")
	}
	*n = int(k)
	return buf[len:], nil
}

func decodeBytes(buf []byte, body *[]byte) ([]byte, error) {
	var err error
	var n int
	buf, err = decodeLength(buf, &n)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return buf, nil
	}
	if len(buf) < n {
		return nil, errors.New("bad body length")
	}
	*body = buf[:n]
	return buf[n:], nil
}

func decodeEfaceSlice[T any](buf []byte, l *[]T, unmarshal func([]byte, interface{}) error) ([]byte, error) {
	var err error
	var total int
	buf, err = decodeLength(buf, &total)
	if err != nil {
		return nil, err
	}
	out := make([]T, total)
	for i := 0; i < total; i++ {
		var body []byte
		buf, err = decodeBytes(buf, &body)
		if err != nil {
			return nil, err
		}
		if body != nil {
			err = unmarshal(body, &out[i])
			if err != nil {
				return nil, err
			}
		}
	}
	*l = out
	return buf, nil
}

func decodeStringSlice(buf []byte, l *[]interface{}) ([]byte, error) {
	var err error
	var total int
	buf, err = decodeLength(buf, &total)
	if err != nil {
		return nil, err
	}
	out := make([]interface{}, total)
	for i := 0; i < total; i++ {
		var body []byte
		buf, err = decodeBytes(buf, &body)
		if err != nil {
			return nil, err
		}
		if body != nil {
			out[i] = string(body)
		}
	}
	*l = out
	return buf, nil

}

func marshalMastNode[K,V comparable](node *mastNode[K,V], marshal func(interface{}) ([]byte, error)) ([]byte, error) {
	var buf []byte
	var err error
	buf, err = appendEfaceSlice(buf, node.Key, marshal)
	if err != nil {
		return nil, err
	}
	buf, err = appendEfaceSlice(buf, node.Value, marshal)
	if err != nil {
		return nil, err
	}

	buf = appendLength(buf, len(node.Link))
	for _, link := range node.Link {
		var str string
		if link != nil {
			var ok bool
			str, ok = link.(string)
			if !ok {
				panic(fmt.Sprintf("expect string link when marshalNode, got:%T", link))
			}
		}
		buf = appendLength(buf, len(str))
		buf = append(buf, str...)
	}
	return buf, nil
}

func unmarshalMastNode[K,V comparable](m *Mast[K,V], buf []byte, node *mastNode[K,V]) error {
	var err error
	buf, err = decodeEfaceSlice(buf, &node.Key, m.unmarshal)
	if err != nil {
		return fmt.Errorf("error when unmarshal node.Key:%s", err)
	}
	buf, err = decodeEfaceSlice(buf, &node.Value, m.unmarshal)
	if err != nil {
		return fmt.Errorf("error when unmarshal node.Value:%s", err)
	}
	buf, err = decodeStringSlice(buf, &node.Link)
	if err != nil {
		return fmt.Errorf("error when unmarshal node.Link:%s", err)
	}
	return nil
}
