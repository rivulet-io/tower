package tower

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/google/uuid"
)

type DataType uint8

const (
	TypeNull DataType = iota
	TypeInt
	TypeFloat
	TypeDecimal
	TypeBigInt
	TypeString
	TypeBool
	TypeTimestamp
	TypeTime
	TypeDuration
	TypeBinary
	TypeUUID
	TypeRoaringBitmap
	TypeRoaringBitmap64
	TypePassword
	TypeJSON
	TypeList
	TypeMap
	TypeSet
	TypeTimeseries
	TypeBloomFilter
	TypeShamirShare
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
	typ       DataType
	payload   []byte
	expiresAt time.Time // zero value means no expiration
}

func (df *DataFrame) Marshal() ([]byte, error) {
	if df == nil {
		return nil, fmt.Errorf("cannot marshal nil DataFrame")
	}

	buf := make([]byte, 1+8+len(df.payload))
	cursor := 0
	buf[cursor] = byte(df.typ)
	cursor++
	binary.BigEndian.PutUint64(buf[cursor:], uint64(df.expiresAt.UnixMilli()))
	cursor += 8
	copy(buf[cursor:], df.payload)

	return buf, nil
}

func UnmarshalDataFrame(data []byte) (*DataFrame, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("data too short to unmarshal DataFrame")
	}

	expirtesAt := time.UnixMilli(int64(binary.BigEndian.Uint64(data[1:9])))

	df := &DataFrame{
		typ:       DataType(data[0]),
		expiresAt: expirtesAt,
	}

	if !expirtesAt.IsZero() && Now().After(expirtesAt) {
		return df, NewDataframeExpiredError("unknown", expirtesAt)
	}

	payload := make([]byte, len(data)-9)
	copy(payload, data[9:])

	df.payload = payload

	return df, nil
}

func NULLDataFrame() *DataFrame {
	return &DataFrame{
		typ:       TypeNull,
		expiresAt: time.Time{},
		payload:   nil,
	}
}

func (df *DataFrame) Type() DataType {
	return df.typ
}

func (df *DataFrame) SetExpiration(t time.Time) {
	df.expiresAt = t
}

func (df *DataFrame) Expiration() time.Time {
	return df.expiresAt
}

func (df *DataFrame) IsExpired(input time.Time) bool {
	if df.expiresAt.IsZero() {
		return false
	}

	return !input.Before(df.expiresAt)
}

func (df *DataFrame) ClearExpiration() {
	df.expiresAt = time.Time{}
}

func (df *DataFrame) SetInt(v int64) error {
	buf := [8]byte{}
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	df.typ = TypeInt
	df.payload = buf[:]
	return nil
}

func (df *DataFrame) Int() (int64, error) {
	if df.typ != TypeInt {
		return 0, &DataFrameError{Op: "Int", Type: df.typ, Msg: "type mismatch"}
	}
	if len(df.payload) != 8 {
		return 0, &DataFrameError{Op: "Int", Type: df.typ, Msg: "invalid payload length"}
	}
	v := int64(binary.BigEndian.Uint64(df.payload))
	return v, nil
}

func (df *DataFrame) SetFloat(v float64) error {
	buf := [8]byte{}
	binary.BigEndian.PutUint64(buf[:], math.Float64bits(v))
	df.typ = TypeFloat
	df.payload = buf[:]
	return nil
}

func (df *DataFrame) Float() (float64, error) {
	if df.typ != TypeFloat {
		return 0, &DataFrameError{Op: "Float", Type: df.typ, Msg: "type mismatch"}
	}
	if len(df.payload) != 8 {
		return 0, &DataFrameError{Op: "Float", Type: df.typ, Msg: "invalid payload length"}
	}
	bits := binary.BigEndian.Uint64(df.payload)
	return math.Float64frombits(bits), nil
}

func (df *DataFrame) SetString(v string) error {
	data := []byte(v)
	length := uint32(len(data))
	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[:4], length)
	copy(buf[4:], data)
	df.typ = TypeString
	df.payload = buf
	return nil
}

func (df *DataFrame) String() (string, error) {
	if df.typ != TypeString {
		return "", &DataFrameError{Op: "String", Type: df.typ, Msg: "type mismatch"}
	}
	if len(df.payload) < 4 {
		return "", &DataFrameError{Op: "String", Type: df.typ, Msg: "payload too short"}
	}
	length := binary.BigEndian.Uint32(df.payload[:4])
	if len(df.payload) != int(4+length) {
		return "", &DataFrameError{Op: "String", Type: df.typ, Msg: "invalid payload length"}
	}
	return string(df.payload[4:]), nil
}

func (df *DataFrame) SetBool(v bool) error {
	var b byte
	if v {
		b = 1
	}
	df.typ = TypeBool
	df.payload = []byte{b}
	return nil
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

func (df *DataFrame) SetTimestamp(v time.Time) error {
	buf := [8]byte{}
	binary.BigEndian.PutUint64(buf[:], uint64(v.UnixNano()))
	df.typ = TypeTimestamp
	df.payload = buf[:]
	return nil
}

func (df *DataFrame) Timestamp() (time.Time, error) {
	if df.typ != TypeTimestamp {
		return time.Time{}, &DataFrameError{Op: "Timestamp", Type: df.typ, Msg: "type mismatch"}
	}
	if len(df.payload) != 8 {
		return time.Time{}, &DataFrameError{Op: "Timestamp", Type: df.typ, Msg: "invalid payload length"}
	}
	nano := int64(binary.BigEndian.Uint64(df.payload))
	return time.Unix(0, nano), nil
}

func (df *DataFrame) SetDuration(v time.Duration) error {
	buf := [8]byte{}
	binary.BigEndian.PutUint64(buf[:], uint64(v.Nanoseconds()))
	df.typ = TypeDuration
	df.payload = buf[:]
	return nil
}

func (df *DataFrame) Duration() (time.Duration, error) {
	if df.typ != TypeDuration {
		return 0, &DataFrameError{Op: "Duration", Type: df.typ, Msg: "type mismatch"}
	}
	if len(df.payload) != 8 {
		return 0, &DataFrameError{Op: "Duration", Type: df.typ, Msg: "invalid payload length"}
	}
	nano := int64(binary.BigEndian.Uint64(df.payload))
	return time.Duration(nano), nil
}

func (df *DataFrame) SetBinary(v []byte) error {
	df.typ = TypeBinary
	df.payload = make([]byte, len(v))
	copy(df.payload, v)
	return nil
}

func (df *DataFrame) Binary() ([]byte, error) {
	if df.typ != TypeBinary {
		return nil, &DataFrameError{Op: "Binary", Type: df.typ, Msg: "type mismatch"}
	}
	data := make([]byte, len(df.payload))
	copy(data, df.payload)
	return data, nil
}

func (df *DataFrame) SetUUID(v *uuid.UUID) error {
	df.typ = TypeUUID
	df.payload = make([]byte, 16)
	copy(df.payload, v[:])
	return nil
}

func (df *DataFrame) UUID() (*uuid.UUID, error) {
	if df.typ != TypeUUID {
		return nil, &DataFrameError{Op: "UUID", Type: df.typ, Msg: "type mismatch"}
	}
	if len(df.payload) != 16 {
		return nil, &DataFrameError{Op: "UUID", Type: df.typ, Msg: "invalid payload length"}
	}
	id := &uuid.UUID{}
	copy(id[:], df.payload)
	return id, nil
}

func (df *DataFrame) SetTime(v time.Time) error {
	df.typ = TypeTime
	df.payload = []byte(v.Format(time.RFC3339Nano))
	return nil
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

// ================================
// BigInt Support
// ================================

type BigIntData struct {
	Value *big.Int
}

func (bid *BigIntData) Marshal() ([]byte, error) {
	if bid.Value == nil {
		return nil, fmt.Errorf("BigInt value cannot be nil")
	}

	bytes := bid.Value.Bytes()
	buf := make([]byte, 1+len(bytes)) // 1 byte for sign + data

	// Store sign (0 = positive/zero, 1 = negative)
	if bid.Value.Sign() < 0 {
		buf[0] = 1
	} else {
		buf[0] = 0
	}

	copy(buf[1:], bytes)
	return buf, nil
}

func UnmarshalDataFrameBigIntData(data []byte) (*BigIntData, error) {
	if len(data) < 1 {
		return nil, &DataFrameError{Op: "UnmarshalDataFrameBigIntData", Type: TypeBigInt, Msg: "data too short"}
	}

	bid := &BigIntData{Value: new(big.Int)}

	// Read sign
	sign := data[0]
	bytes := data[1:]

	bid.Value.SetBytes(bytes)

	// Apply sign
	if sign == 1 {
		bid.Value.Neg(bid.Value)
	}

	return bid, nil
}

func (df *DataFrame) SetBigInt(value *big.Int) error {
	if value == nil {
		return &DataFrameError{
			Op:   "SetBigInt",
			Type: TypeBigInt,
			Msg:  "value cannot be nil",
		}
	}

	data := &BigIntData{Value: new(big.Int).Set(value)}
	buf, err := data.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal BigInt data: %w", err)
	}

	df.typ = TypeBigInt
	df.payload = buf
	return nil
}

func (df *DataFrame) BigInt() (*big.Int, error) {
	if df.typ != TypeBigInt {
		return nil, &DataFrameError{Op: "BigInt", Type: df.typ, Msg: "type mismatch"}
	}

	data, err := UnmarshalDataFrameBigIntData(df.payload)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal BigInt data: %w", err)
	}

	return data.Value, nil
}

// ================================
// Decimal Support (Fixed-Point Arithmetic)
// ================================

type DecimalData struct {
	Coefficient *big.Int // The significand (mantissa) as big.Int
	Scale       int32    // Number of decimal places (0 = integer)
}

func (dd *DecimalData) Marshal() ([]byte, error) {
	if dd.Coefficient == nil {
		return nil, fmt.Errorf("Decimal coefficient cannot be nil")
	}

	coeffBytes := dd.Coefficient.Bytes()
	scaleBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(scaleBytes, uint32(dd.Scale))

	// Format: sign(1) + coeff_length(4) + coeff_bytes + scale(4)
	buf := make([]byte, 1+4+len(coeffBytes)+4)

	// Store sign (0 = positive/zero, 1 = negative)
	if dd.Coefficient.Sign() < 0 {
		buf[0] = 1
	} else {
		buf[0] = 0
	}

	// Store coefficient length
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(coeffBytes)))

	// Store coefficient bytes
	copy(buf[5:5+len(coeffBytes)], coeffBytes)

	// Store scale
	copy(buf[5+len(coeffBytes):], scaleBytes)

	return buf, nil
}

func UnmarshalDataFrameDecimalData(data []byte) (*DecimalData, error) {
	if len(data) < 9 { // Minimum: sign(1) + length(4) + scale(4), coeff bytes can be 0
		return nil, &DataFrameError{Op: "UnmarshalDataFrameDecimalData", Type: TypeDecimal, Msg: "data too short"}
	}

	dd := &DecimalData{Coefficient: new(big.Int)}

	// Read sign
	sign := data[0]

	// Read coefficient length
	coeffLen := binary.BigEndian.Uint32(data[1:5])
	if len(data) < int(5+coeffLen+4) {
		return nil, &DataFrameError{Op: "UnmarshalDataFrameDecimalData", Type: TypeDecimal, Msg: "invalid data length"}
	}

	// Read coefficient bytes
	coeffBytes := data[5 : 5+coeffLen]
	dd.Coefficient.SetBytes(coeffBytes)

	// Apply sign
	if sign == 1 {
		dd.Coefficient.Neg(dd.Coefficient)
	}

	// Read scale
	scaleStart := 5 + coeffLen
	dd.Scale = int32(binary.BigEndian.Uint32(data[scaleStart : scaleStart+4]))

	return dd, nil
}

func (df *DataFrame) SetDecimal(coefficient *big.Int, scale int32) error {
	if coefficient == nil {
		return &DataFrameError{
			Op:   "SetDecimal",
			Type: TypeDecimal,
			Msg:  "coefficient cannot be nil",
		}
	}
	if scale < 0 {
		return &DataFrameError{
			Op:   "SetDecimal",
			Type: TypeDecimal,
			Msg:  "scale cannot be negative",
		}
	}

	data := &DecimalData{
		Coefficient: new(big.Int).Set(coefficient),
		Scale:       scale,
	}

	buf, err := data.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal decimal data: %w", err)
	}

	df.typ = TypeDecimal
	df.payload = buf
	return nil
}

func (df *DataFrame) Decimal() (coefficient *big.Int, scale int32, err error) {
	if df.typ != TypeDecimal {
		return nil, 0, &DataFrameError{Op: "Decimal", Type: df.typ, Msg: "type mismatch"}
	}

	data, err := UnmarshalDataFrameDecimalData(df.payload)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to unmarshal decimal data: %w", err)
	}

	return new(big.Int).Set(data.Coefficient), data.Scale, nil
}

// ================================
// Shamir Secret Share Support
// ================================

type ShamirShareData struct {
	Shares map[byte][]byte // Map of share ID to share data
}

func (ssd *ShamirShareData) Marshal() ([]byte, error) {
	if ssd.Shares == nil {
		return nil, fmt.Errorf("shamir shares cannot be nil")
	}

	// Calculate total size needed
	// Format: num_shares(4) + [share_id(1) + share_length(4) + share_bytes]*num_shares
	totalSize := 4 // for num_shares
	for _, share := range ssd.Shares {
		totalSize += 1 + 4 + len(share) // share_id + share_length + share_bytes
	}

	buf := make([]byte, totalSize)
	offset := 0

	// Store number of shares
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(ssd.Shares)))
	offset += 4

	// Store each share
	for shareID, share := range ssd.Shares {
		// Store share ID
		buf[offset] = shareID
		offset++

		// Store share length
		binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(share)))
		offset += 4

		// Store share bytes
		copy(buf[offset:offset+len(share)], share)
		offset += len(share)
	}

	return buf, nil
}

func UnmarshalDataFrameShamirShareData(data []byte) (*ShamirShareData, error) {
	if len(data) < 4 {
		return nil, &DataFrameError{Op: "UnmarshalDataFrameShamirShareData", Type: TypeShamirShare, Msg: "data too short"}
	}

	ssd := &ShamirShareData{Shares: make(map[byte][]byte)}
	offset := 0

	// Read number of shares
	numShares := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Read each share
	for i := uint32(0); i < numShares; i++ {
		if offset >= len(data) {
			return nil, &DataFrameError{Op: "UnmarshalDataFrameShamirShareData", Type: TypeShamirShare, Msg: "invalid data length"}
		}

		// Read share ID
		shareID := data[offset]
		offset++

		// Read share length
		if offset+4 > len(data) {
			return nil, &DataFrameError{Op: "UnmarshalDataFrameShamirShareData", Type: TypeShamirShare, Msg: "invalid data length"}
		}
		shareLength := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		// Read share bytes
		if offset+int(shareLength) > len(data) {
			return nil, &DataFrameError{Op: "UnmarshalDataFrameShamirShareData", Type: TypeShamirShare, Msg: "invalid data length"}
		}
		share := make([]byte, shareLength)
		copy(share, data[offset:offset+int(shareLength)])
		offset += int(shareLength)

		ssd.Shares[shareID] = share
	}

	return ssd, nil
}

func (df *DataFrame) SetShamirShare(shares map[byte][]byte) error {
	if shares == nil {
		return &DataFrameError{
			Op:   "SetShamirShare",
			Type: TypeShamirShare,
			Msg:  "shares cannot be nil",
		}
	}

	data := &ShamirShareData{Shares: make(map[byte][]byte)}
	for shareID, share := range shares {
		shareCopy := make([]byte, len(share))
		copy(shareCopy, share)
		data.Shares[shareID] = shareCopy
	}

	buf, err := data.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal Shamir share data: %w", err)
	}

	df.typ = TypeShamirShare
	df.payload = buf
	return nil
}

func (df *DataFrame) ShamirShare() (map[byte][]byte, error) {
	if df.typ != TypeShamirShare {
		return nil, &DataFrameError{Op: "ShamirShare", Type: df.typ, Msg: "type mismatch"}
	}

	data, err := UnmarshalDataFrameShamirShareData(df.payload)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal Shamir share data: %w", err)
	}

	// Return a copy of the shares to prevent mutation
	shares := make(map[byte][]byte)
	for shareID, share := range data.Shares {
		shareCopy := make([]byte, len(share))
		copy(shareCopy, share)
		shares[shareID] = shareCopy
	}

	return shares, nil
}

func (df *DataFrame) SetRoaringBitmap(v *roaring.Bitmap) error {
	if v == nil {
		return &DataFrameError{
			Op:   "SetRoaringBitmap",
			Type: TypeNull,
			Msg:  "bitmap cannot be nil",
		}
	}

	data, err := v.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal roaring bitmap: %w", err)
	}

	df.typ = TypeRoaringBitmap
	df.payload = data

	return nil
}

func (df *DataFrame) RoaringBitmap() (*roaring.Bitmap, error) {
	if df.typ != TypeRoaringBitmap {
		return nil, &DataFrameError{Op: "RoaringBitmap", Type: df.typ, Msg: "type mismatch"}
	}

	bitmap := roaring.New()
	if err := bitmap.UnmarshalBinary(df.payload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal roaring bitmap: %w", err)
	}

	return bitmap, nil
}

func (df *DataFrame) SetRoaringBitmap64(v *roaring64.Bitmap) error {
	if v == nil {
		return &DataFrameError{
			Op:   "SetRoaringBitmap64",
			Type: TypeNull,
			Msg:  "bitmap cannot be nil",
		}
	}

	data, err := v.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal roaring64 bitmap: %w", err)
	}

	df.typ = TypeRoaringBitmap64
	df.payload = data
	return nil
}

func (df *DataFrame) RoaringBitmap64() (*roaring64.Bitmap, error) {
	if df.typ != TypeRoaringBitmap64 {
		return nil, &DataFrameError{Op: "RoaringBitmap64", Type: df.typ, Msg: "type mismatch"}
	}

	bitmap := roaring64.New()
	if err := bitmap.UnmarshalBinary(df.payload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal roaring64 bitmap: %w", err)
	}

	return bitmap, nil
}

type PasswordData struct {
	Hash      []byte            `json:"hash"`
	Salt      []byte            `json:"salt"`
	Algorithm PasswordAlgorithm `json:"algorithm"`
}

func (df *DataFrame) SetPassword(algo PasswordAlgorithm, hash []byte, salt []byte) error {
	if len(hash) == 0 || len(salt) == 0 {
		return &DataFrameError{
			Op:   "SetPassword",
			Type: TypePassword,
			Msg:  "hash and salt cannot be empty",
		}
	}

	value := &PasswordData{
		Algorithm: algo,
		Hash:      make([]byte, len(hash)),
		Salt:      make([]byte, len(salt)),
	}
	copy(value.Hash, hash)
	copy(value.Salt, salt)

	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal password data: %w", err)
	}

	df.typ = TypePassword
	df.payload = data

	return nil
}

func (df *DataFrame) Password() (algo PasswordAlgorithm, hash []byte, salt []byte, err error) {
	if df.typ != TypePassword {
		return 0, nil, nil, &DataFrameError{Op: "Password", Type: df.typ, Msg: "type mismatch"}
	}

	value := &PasswordData{}
	if err := json.Unmarshal(df.payload, value); err != nil {
		return 0, nil, nil, fmt.Errorf("failed to unmarshal password data: %w", err)
	}

	return value.Algorithm, value.Hash, value.Salt, nil
}
