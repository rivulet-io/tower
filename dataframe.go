package tower

import (
	"encoding/binary"
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

func (df *DataFrame) Int() (int64, bool) {
	if df.typ != TypeInt || len(df.payload) != 8 {
		return 0, false
	}
	v := int64(binary.LittleEndian.Uint64(df.payload))
	return v, true
}

func (df *DataFrame) SetFloat(v float64) {
	buf := [8]byte{}
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v))
	df.typ = TypeFloat
	df.payload = buf[:]
}

func (df *DataFrame) Float() (float64, bool) {
	if df.typ != TypeFloat || len(df.payload) != 8 {
		return 0, false
	}
	bits := binary.LittleEndian.Uint64(df.payload)
	return math.Float64frombits(bits), true
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

func (df *DataFrame) String() (string, bool) {
	if df.typ != TypeString || len(df.payload) < 4 {
		return "", false
	}
	length := binary.LittleEndian.Uint32(df.payload[:4])
	if len(df.payload) != int(4+length) {
		return "", false
	}
	return string(df.payload[4:]), true
}

func (df *DataFrame) SetBool(v bool) {
	var b byte
	if v {
		b = 1
	}
	df.typ = TypeBool
	df.payload = []byte{b}
}

func (df *DataFrame) Bool() (bool, bool) {
	if df.typ != TypeBool || len(df.payload) != 1 {
		return false, false
	}
	return df.payload[0] != 0, true
}

func (df *DataFrame) SetTimestamp(v time.Time) {
	buf := [8]byte{}
	binary.LittleEndian.PutUint64(buf[:], uint64(v.UnixNano()))
	df.typ = TypeTimestamp
	df.payload = buf[:]
}

func (df *DataFrame) Timestamp() (time.Time, bool) {
	if df.typ != TypeTimestamp || len(df.payload) != 8 {
		return time.Time{}, false
	}
	nano := int64(binary.LittleEndian.Uint64(df.payload))
	return time.Unix(0, nano), true
}

func (df *DataFrame) SetDuration(v time.Duration) {
	buf := [8]byte{}
	binary.LittleEndian.PutUint64(buf[:], uint64(v.Nanoseconds()))
	df.typ = TypeDuration
	df.payload = buf[:]
}

func (df *DataFrame) Duration() (time.Duration, bool) {
	if df.typ != TypeDuration || len(df.payload) != 8 {
		return 0, false
	}
	nano := int64(binary.LittleEndian.Uint64(df.payload))
	return time.Duration(nano), true
}

func (df *DataFrame) SetBinary(v []byte) {
	df.typ = TypeBinary
	df.payload = make([]byte, len(v))
	copy(df.payload, v)
}

func (df *DataFrame) Binary() ([]byte, bool) {
	if df.typ != TypeBinary {
		return nil, false
	}
	data := make([]byte, len(df.payload))
	copy(data, df.payload)
	return data, true
}

func (df *DataFrame) SetUUID(v [16]byte) {
	df.typ = TypeUUID
	df.payload = make([]byte, 16)
	copy(df.payload, v[:])
}

func (df *DataFrame) UUID() ([16]byte, bool) {
	if df.typ != TypeUUID || len(df.payload) != 16 {
		return [16]byte{}, false
	}
	var uuid [16]byte
	copy(uuid[:], df.payload)
	return uuid, true
}

func (df *DataFrame) SetTime(v time.Time) {
	df.typ = TypeTime
	df.payload = []byte(v.Format(time.RFC3339Nano))
}

func (df *DataFrame) Time() (time.Time, bool) {
	if df.typ != TypeTime {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, string(df.payload))
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}
