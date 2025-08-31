package tower

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

type DataType uint8

const (
	TypeNull DataType = iota
	TypeInt
	TypeFloat
	TypeString
	TypeBool
	TypeTimestamp
	TypeTime
	TypeDuration
	TypeBinary
	TypeUUID
	TypeJSON
	TypeList
	TypeMap
	TypeSet
)

type DataFrameError struct {
	Op   string
	Type DataType
	Msg  string
}

func (e *DataFrameError) Error() string {
	return fmt.Sprintf("dataframe %s error for type %v: %s", e.Op, e.Type, e.Msg)
}

type DataFrame struct {
	typ     DataType
	payload []byte
}

func NULLDataFrame() *DataFrame {
	return &DataFrame{
		typ:     TypeNull,
		payload: nil,
	}
}

func (df *DataFrame) Type() DataType {
	return df.typ
}

func (df *DataFrame) SetInt(v int64) {
	buf := [8]byte{}
	binary.LittleEndian.PutUint64(buf[:], uint64(v))
	df.typ = TypeInt
	df.payload = buf[:]
}

func (df *DataFrame) Int() (int64, error) {
	if df.typ != TypeInt {
		return 0, &DataFrameError{Op: "Int", Type: df.typ, Msg: "type mismatch"}
	}
	if len(df.payload) != 8 {
		return 0, &DataFrameError{Op: "Int", Type: df.typ, Msg: "invalid payload length"}
	}
	v := int64(binary.LittleEndian.Uint64(df.payload))
	return v, nil
}

func (df *DataFrame) SetFloat(v float64) {
	buf := [8]byte{}
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v))
	df.typ = TypeFloat
	df.payload = buf[:]
}

func (df *DataFrame) Float() (float64, error) {
	if df.typ != TypeFloat {
		return 0, &DataFrameError{Op: "Float", Type: df.typ, Msg: "type mismatch"}
	}
	if len(df.payload) != 8 {
		return 0, &DataFrameError{Op: "Float", Type: df.typ, Msg: "invalid payload length"}
	}
	bits := binary.LittleEndian.Uint64(df.payload)
	return math.Float64frombits(bits), nil
}

func (df *DataFrame) SetString(v string) {
	data := []byte(v)
	length := uint32(len(data))
	buf := make([]byte, 4+len(data))
	binary.LittleEndian.PutUint32(buf[:4], length)
	copy(buf[4:], data)
	df.typ = TypeString
	df.payload = buf
}

func (df *DataFrame) String() (string, error) {
	if df.typ != TypeString {
		return "", &DataFrameError{Op: "String", Type: df.typ, Msg: "type mismatch"}
	}
	if len(df.payload) < 4 {
		return "", &DataFrameError{Op: "String", Type: df.typ, Msg: "payload too short"}
	}
	length := binary.LittleEndian.Uint32(df.payload[:4])
	if len(df.payload) != int(4+length) {
		return "", &DataFrameError{Op: "String", Type: df.typ, Msg: "invalid payload length"}
	}
	return string(df.payload[4:]), nil
}

func (df *DataFrame) SetBool(v bool) {
	var b byte
	if v {
		b = 1
	}
	df.typ = TypeBool
	df.payload = []byte{b}
}

func (df *DataFrame) Bool() (bool, error) {
	if df.typ != TypeBool {
		return false, &DataFrameError{Op: "Bool", Type: df.typ, Msg: "type mismatch"}
	}
	if len(df.payload) != 1 {
		return false, &DataFrameError{Op: "Bool", Type: df.typ, Msg: "invalid payload length"}
	}
	return df.payload[0] != 0, nil
}

func (df *DataFrame) SetTimestamp(v time.Time) {
	buf := [8]byte{}
	binary.LittleEndian.PutUint64(buf[:], uint64(v.UnixNano()))
	df.typ = TypeTimestamp
	df.payload = buf[:]
}

func (df *DataFrame) Timestamp() (time.Time, error) {
	if df.typ != TypeTimestamp {
		return time.Time{}, &DataFrameError{Op: "Timestamp", Type: df.typ, Msg: "type mismatch"}
	}
	if len(df.payload) != 8 {
		return time.Time{}, &DataFrameError{Op: "Timestamp", Type: df.typ, Msg: "invalid payload length"}
	}
	nano := int64(binary.LittleEndian.Uint64(df.payload))
	return time.Unix(0, nano), nil
}

func (df *DataFrame) SetDuration(v time.Duration) {
	buf := [8]byte{}
	binary.LittleEndian.PutUint64(buf[:], uint64(v.Nanoseconds()))
	df.typ = TypeDuration
	df.payload = buf[:]
}

func (df *DataFrame) Duration() (time.Duration, error) {
	if df.typ != TypeDuration {
		return 0, &DataFrameError{Op: "Duration", Type: df.typ, Msg: "type mismatch"}
	}
	if len(df.payload) != 8 {
		return 0, &DataFrameError{Op: "Duration", Type: df.typ, Msg: "invalid payload length"}
	}
	nano := int64(binary.LittleEndian.Uint64(df.payload))
	return time.Duration(nano), nil
}

func (df *DataFrame) SetBinary(v []byte) {
	df.typ = TypeBinary
	df.payload = make([]byte, len(v))
	copy(df.payload, v)
}

func (df *DataFrame) Binary() ([]byte, error) {
	if df.typ != TypeBinary {
		return nil, &DataFrameError{Op: "Binary", Type: df.typ, Msg: "type mismatch"}
	}
	data := make([]byte, len(df.payload))
	copy(data, df.payload)
	return data, nil
}

func (df *DataFrame) SetUUID(v [16]byte) {
	df.typ = TypeUUID
	df.payload = make([]byte, 16)
	copy(df.payload, v[:])
}

func (df *DataFrame) UUID() ([16]byte, error) {
	if df.typ != TypeUUID {
		return [16]byte{}, &DataFrameError{Op: "UUID", Type: df.typ, Msg: "type mismatch"}
	}
	if len(df.payload) != 16 {
		return [16]byte{}, &DataFrameError{Op: "UUID", Type: df.typ, Msg: "invalid payload length"}
	}
	var uuid [16]byte
	copy(uuid[:], df.payload)
	return uuid, nil
}

func (df *DataFrame) SetTime(v time.Time) {
	df.typ = TypeTime
	df.payload = []byte(v.Format(time.RFC3339Nano))
}

func (df *DataFrame) Time() (time.Time, error) {
	if df.typ != TypeTime {
		return time.Time{}, &DataFrameError{Op: "Time", Type: df.typ, Msg: "type mismatch"}
	}
	t, err := time.Parse(time.RFC3339Nano, string(df.payload))
	if err != nil {
		return time.Time{}, &DataFrameError{Op: "Time", Type: df.typ, Msg: err.Error()}
	}
	return t, nil
}
