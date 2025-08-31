package tower

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type PrimitiveData interface {
	Type() DataType
	Int() (int64, error)
	Float() (float64, error)
	String() (string, error)
	Bool() (bool, error)
	Timestamp() (int64, error)
	Time() (time.Time, error)
	Duration() (time.Duration, error)
	Binary() ([]byte, error)
	UUID() (uuid.UUID, error)
}

type PrimitiveInt int64

func (p PrimitiveInt) Type() DataType {
	return TypeInt
}

func (p PrimitiveInt) Int() (int64, error) {
	return int64(p), nil
}

func (p PrimitiveInt) Float() (float64, error) {
	return 0, fmt.Errorf("this is not a float, type is int")
}

func (p PrimitiveInt) String() (string, error) {
	return "", fmt.Errorf("this is not a string, type is int")
}

func (p PrimitiveInt) Bool() (bool, error) {
	return false, fmt.Errorf("this is not a bool, type is int")
}

func (p PrimitiveInt) Timestamp() (int64, error) {
	return 0, fmt.Errorf("this is not a timestamp, type is int")
}

func (p PrimitiveInt) Time() (time.Time, error) {
	return time.Time{}, fmt.Errorf("this is not a time, type is int")
}

func (p PrimitiveInt) Duration() (time.Duration, error) {
	return 0, fmt.Errorf("this is not a duration, type is int")
}

func (p PrimitiveInt) Binary() ([]byte, error) {
	return nil, fmt.Errorf("this is not a binary, type is int")
}

func (p PrimitiveInt) UUID() (uuid.UUID, error) {
	return uuid.UUID{}, fmt.Errorf("this is not a UUID, type is int")
}

type PrimitiveFloat float64

func (p PrimitiveFloat) Type() DataType {
	return TypeFloat
}

func (p PrimitiveFloat) Int() (int64, error) {
	return 0, fmt.Errorf("this is not an int, type is float")
}

func (p PrimitiveFloat) Float() (float64, error) {
	return float64(p), nil
}

func (p PrimitiveFloat) String() (string, error) {
	return "", fmt.Errorf("this is not a string, type is float")
}

func (p PrimitiveFloat) Bool() (bool, error) {
	return false, fmt.Errorf("this is not a bool, type is float")
}

func (p PrimitiveFloat) Timestamp() (int64, error) {
	return 0, fmt.Errorf("this is not a timestamp, type is float")
}

func (p PrimitiveFloat) Time() (time.Time, error) {
	return time.Time{}, fmt.Errorf("this is not a time, type is float")
}

func (p PrimitiveFloat) Duration() (time.Duration, error) {
	return 0, fmt.Errorf("this is not a duration, type is float")
}

func (p PrimitiveFloat) Binary() ([]byte, error) {
	return nil, fmt.Errorf("this is not a binary, type is float")
}

func (p PrimitiveFloat) UUID() (uuid.UUID, error) {
	return uuid.UUID{}, fmt.Errorf("this is not a UUID, type is float")
}

type PrimitiveString string

func (p PrimitiveString) Type() DataType {
	return TypeString
}

func (p PrimitiveString) Int() (int64, error) {
	return 0, fmt.Errorf("this is not an int, type is string")
}

func (p PrimitiveString) Float() (float64, error) {
	return 0, fmt.Errorf("this is not a float, type is string")
}

func (p PrimitiveString) String() (string, error) {
	return string(p), nil
}

func (p PrimitiveString) Bool() (bool, error) {
	return false, fmt.Errorf("this is not a bool, type is string")
}

func (p PrimitiveString) Timestamp() (int64, error) {
	return 0, fmt.Errorf("this is not a timestamp, type is string")
}

func (p PrimitiveString) Time() (time.Time, error) {
	return time.Time{}, fmt.Errorf("this is not a time, type is string")
}

func (p PrimitiveString) Duration() (time.Duration, error) {
	return 0, fmt.Errorf("this is not a duration, type is string")
}

func (p PrimitiveString) Binary() ([]byte, error) {
	return nil, fmt.Errorf("this is not a binary, type is string")
}

func (p PrimitiveString) UUID() (uuid.UUID, error) {
	return uuid.UUID{}, fmt.Errorf("this is not a UUID, type is string")
}

type PrimitiveBool bool

func (p PrimitiveBool) Type() DataType {
	return TypeBool
}

func (p PrimitiveBool) Int() (int64, error) {
	return 0, fmt.Errorf("this is not an int, type is bool")
}

func (p PrimitiveBool) Float() (float64, error) {
	return 0, fmt.Errorf("this is not a float, type is bool")
}

func (p PrimitiveBool) String() (string, error) {
	return "", fmt.Errorf("this is not a string, type is bool")
}

func (p PrimitiveBool) Bool() (bool, error) {
	return bool(p), nil
}

func (p PrimitiveBool) Timestamp() (int64, error) {
	return 0, fmt.Errorf("this is not a timestamp, type is bool")
}

func (p PrimitiveBool) Time() (time.Time, error) {
	return time.Time{}, fmt.Errorf("this is not a time, type is bool")
}

func (p PrimitiveBool) Duration() (time.Duration, error) {
	return 0, fmt.Errorf("this is not a duration, type is bool")
}

func (p PrimitiveBool) Binary() ([]byte, error) {
	return nil, fmt.Errorf("this is not a binary, type is bool")
}

func (p PrimitiveBool) UUID() (uuid.UUID, error) {
	return uuid.UUID{}, fmt.Errorf("this is not a UUID, type is bool")
}

type PrimitiveBinary []byte

func (p PrimitiveBinary) Type() DataType {
	return TypeBinary
}

func (p PrimitiveBinary) Int() (int64, error) {
	return 0, fmt.Errorf("this is not an int, type is binary")
}

func (p PrimitiveBinary) Float() (float64, error) {
	return 0, fmt.Errorf("this is not a float, type is binary")
}

func (p PrimitiveBinary) String() (string, error) {
	return "", fmt.Errorf("this is not a string, type is binary")
}

func (p PrimitiveBinary) Bool() (bool, error) {
	return false, fmt.Errorf("this is not a bool, type is binary")
}

func (p PrimitiveBinary) Timestamp() (int64, error) {
	return 0, fmt.Errorf("this is not a timestamp, type is binary")
}

func (p PrimitiveBinary) Time() (time.Time, error) {
	return time.Time{}, fmt.Errorf("this is not a time, type is binary")
}

func (p PrimitiveBinary) Duration() (time.Duration, error) {
	return 0, fmt.Errorf("this is not a duration, type is binary")
}

func (p PrimitiveBinary) Binary() ([]byte, error) {
	result := make([]byte, len(p))
	copy(result, p)
	return result, nil
}

func (p PrimitiveBinary) UUID() (uuid.UUID, error) {
	return uuid.UUID{}, fmt.Errorf("this is not a UUID, type is binary")
}

type PrimitiveTimestamp int64

func (p PrimitiveTimestamp) Type() DataType {
	return TypeTimestamp
}

func (p PrimitiveTimestamp) Int() (int64, error) {
	return 0, fmt.Errorf("this is not an int, type is timestamp")
}

func (p PrimitiveTimestamp) Float() (float64, error) {
	return 0, fmt.Errorf("this is not a float, type is timestamp")
}

func (p PrimitiveTimestamp) String() (string, error) {
	return "", fmt.Errorf("this is not a string, type is timestamp")
}

func (p PrimitiveTimestamp) Bool() (bool, error) {
	return false, fmt.Errorf("this is not a bool, type is timestamp")
}

func (p PrimitiveTimestamp) Timestamp() (int64, error) {
	return int64(p), nil
}

func (p PrimitiveTimestamp) Time() (time.Time, error) {
	return time.Time{}, fmt.Errorf("this is not a time, type is timestamp")
}

func (p PrimitiveTimestamp) Duration() (time.Duration, error) {
	return 0, fmt.Errorf("this is not a duration, type is timestamp")
}

func (p PrimitiveTimestamp) Binary() ([]byte, error) {
	return nil, fmt.Errorf("this is not a binary, type is timestamp")
}

func (p PrimitiveTimestamp) UUID() (uuid.UUID, error) {
	return uuid.UUID{}, fmt.Errorf("this is not a UUID, type is timestamp")
}

type PrimitiveTime time.Time

func (p PrimitiveTime) Type() DataType {
	return TypeTime
}

func (p PrimitiveTime) Int() (int64, error) {
	return 0, fmt.Errorf("this is not an int, type is time")
}

func (p PrimitiveTime) Float() (float64, error) {
	return 0, fmt.Errorf("this is not a float, type is time")
}

func (p PrimitiveTime) String() (string, error) {
	return "", fmt.Errorf("this is not a string, type is time")
}

func (p PrimitiveTime) Bool() (bool, error) {
	return false, fmt.Errorf("this is not a bool, type is time")
}

func (p PrimitiveTime) Timestamp() (int64, error) {
	return 0, fmt.Errorf("this is not a timestamp, type is time")
}

func (p PrimitiveTime) Time() (time.Time, error) {
	return time.Time(p), nil
}

func (p PrimitiveTime) Duration() (time.Duration, error) {
	return 0, fmt.Errorf("this is not a duration, type is time")
}

func (p PrimitiveTime) Binary() ([]byte, error) {
	return nil, fmt.Errorf("this is not a binary, type is time")
}

func (p PrimitiveTime) UUID() (uuid.UUID, error) {
	return uuid.UUID{}, fmt.Errorf("this is not a UUID, type is time")
}

type PrimitiveDuration time.Duration

func (p PrimitiveDuration) Type() DataType {
	return TypeDuration
}

func (p PrimitiveDuration) Int() (int64, error) {
	return 0, fmt.Errorf("this is not an int, type is duration")
}

func (p PrimitiveDuration) Float() (float64, error) {
	return 0, fmt.Errorf("this is not a float, type is duration")
}

func (p PrimitiveDuration) String() (string, error) {
	return "", fmt.Errorf("this is not a string, type is duration")
}

func (p PrimitiveDuration) Bool() (bool, error) {
	return false, fmt.Errorf("this is not a bool, type is duration")
}

func (p PrimitiveDuration) Timestamp() (int64, error) {
	return 0, fmt.Errorf("this is not a timestamp, type is duration")
}

func (p PrimitiveDuration) Time() (time.Time, error) {
	return time.Time{}, fmt.Errorf("this is not a time, type is duration")
}

func (p PrimitiveDuration) Duration() (time.Duration, error) {
	return time.Duration(p), nil
}

func (p PrimitiveDuration) Binary() ([]byte, error) {
	return nil, fmt.Errorf("this is not a binary, type is duration")
}

func (p PrimitiveDuration) UUID() (uuid.UUID, error) {
	return uuid.UUID{}, fmt.Errorf("this is not a UUID, type is duration")
}

type PrimitiveUUID uuid.UUID

func (p PrimitiveUUID) Type() DataType {
	return TypeUUID
}

func (p PrimitiveUUID) Int() (int64, error) {
	return 0, fmt.Errorf("this is not an int, type is UUID")
}

func (p PrimitiveUUID) Float() (float64, error) {
	return 0, fmt.Errorf("this is not a float, type is UUID")
}

func (p PrimitiveUUID) String() (string, error) {
	return "", fmt.Errorf("this is not a string, type is UUID")
}

func (p PrimitiveUUID) Bool() (bool, error) {
	return false, fmt.Errorf("this is not a bool, type is UUID")
}

func (p PrimitiveUUID) Timestamp() (int64, error) {
	return 0, fmt.Errorf("this is not a timestamp, type is UUID")
}

func (p PrimitiveUUID) Time() (time.Time, error) {
	return time.Time{}, fmt.Errorf("this is not a time, type is UUID")
}

func (p PrimitiveUUID) Duration() (time.Duration, error) {
	return 0, fmt.Errorf("this is not a duration, type is UUID")
}

func (p PrimitiveUUID) Binary() ([]byte, error) {
	return nil, fmt.Errorf("this is not a binary, type is UUID")
}

func (p PrimitiveUUID) UUID() (uuid.UUID, error) {
	return uuid.UUID(p), nil
}
