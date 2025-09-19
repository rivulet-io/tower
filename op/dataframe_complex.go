package op

import (
	"encoding/binary"
	"fmt"
	"time"
)

type ListData struct {
	Prefix    string
	HeadIndex int64
	TailIndex int64
	Length    int64
}

func (ld *ListData) Marshal() ([]byte, error) {
	buf := make([]byte, 8+8+8+len(ld.Prefix))
	binary.BigEndian.PutUint64(buf[0:8], uint64(ld.HeadIndex))
	binary.BigEndian.PutUint64(buf[8:16], uint64(ld.TailIndex))
	binary.BigEndian.PutUint64(buf[16:24], uint64(ld.Length))
	copy(buf[24:], []byte(ld.Prefix))
	return buf, nil
}

func UnmarshalDataFrameListData(data []byte) (*ListData, error) {
	if len(data) < 24 {
		return nil, &DataFrameError{Op: "UnmarshalDataFrameListData", Type: TypeList, Msg: "data too short"}
	}
	ld := &ListData{}
	ld.HeadIndex = int64(binary.BigEndian.Uint64(data[0:8]))
	ld.TailIndex = int64(binary.BigEndian.Uint64(data[8:16]))
	ld.Length = int64(binary.BigEndian.Uint64(data[16:24]))
	ld.Prefix = string(data[24:])
	return ld, nil
}

func (df *DataFrame) SetList(data *ListData) error {
	if data == nil {
		return &DataFrameError{
			Op:   "SetList",
			Type: TypeList,
			Msg:  "data cannot be nil",
		}
	}

	buf, err := data.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal list data: %w", err)
	}

	df.typ = TypeList
	df.payload = buf

	return nil
}

func (df *DataFrame) List() (*ListData, error) {
	if df.typ != TypeList {
		return nil, &DataFrameError{Op: "List", Type: df.typ, Msg: "type mismatch"}
	}

	value, err := UnmarshalDataFrameListData(df.payload)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal list data: %w", err)
	}

	return value, nil
}

const ListTypeMarker = "{:list:}"

func MakeListEntryKey(prefix string) []byte {
	buf := make([]byte, len(prefix)+len(ListTypeMarker)+1)
	copy(buf, []byte(prefix))
	buf[len(prefix)] = ':'
	copy(buf[len(prefix)+1:], []byte(ListTypeMarker))
	return buf
}

func MakeListItemKey(prefix string, index int64) []byte {
	buf := make([]byte, len(prefix)+len(ListTypeMarker)+8+2)
	copy(buf, []byte(prefix))
	buf[len(prefix)] = ':'
	copy(buf[len(prefix)+1:], []byte(ListTypeMarker))
	buf[len(prefix)+1+len(ListTypeMarker)] = ':'
	binary.BigEndian.PutUint64(buf[len(prefix)+1+len(ListTypeMarker)+1:], uint64(index))
	return buf
}

type SetData struct {
	Prefix string
	Count  uint64
}

func (sd *SetData) Marshal() ([]byte, error) {
	buf := make([]byte, 8+len(sd.Prefix))
	binary.BigEndian.PutUint64(buf[0:8], sd.Count)
	copy(buf[8:], []byte(sd.Prefix))
	return buf, nil
}

func UnmarshalDataFrameSetData(data []byte) (*SetData, error) {
	if len(data) < 8 {
		return nil, &DataFrameError{Op: "UnmarshalDataFrameSetData", Type: TypeSet, Msg: "data too short"}
	}
	sd := &SetData{}
	sd.Count = binary.BigEndian.Uint64(data[0:8])
	sd.Prefix = string(data[8:])
	return sd, nil
}

func (df *DataFrame) SetSet(data *SetData) error {
	if data == nil {
		return &DataFrameError{
			Op:   "SetSet",
			Type: TypeSet,
			Msg:  "data cannot be nil",
		}
	}

	buf, err := data.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal set data: %w", err)
	}

	df.typ = TypeSet
	df.payload = buf

	return nil
}

func (df *DataFrame) Set() (*SetData, error) {
	if df.typ != TypeSet {
		return nil, &DataFrameError{Op: "Set", Type: df.typ, Msg: "type mismatch"}
	}

	value, err := UnmarshalDataFrameSetData(df.payload)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal set data: %w", err)
	}

	return value, nil
}

const SetTypeMarker = "{:set:}"

func MakeSetEntryKey(prefix string) []byte {
	buf := make([]byte, len(prefix)+len(SetTypeMarker)+1)
	copy(buf, []byte(prefix))
	buf[len(prefix)] = ':'
	copy(buf[len(prefix)+1:], []byte(SetTypeMarker))
	return buf
}

func MakeSetItemKey(prefix string, member string) []byte {
	buf := make([]byte, len(prefix)+len(SetTypeMarker)+len(member)+2)
	copy(buf, []byte(prefix))
	buf[len(prefix)] = ':'
	copy(buf[len(prefix)+1:], []byte(SetTypeMarker))
	buf[len(prefix)+1+len(SetTypeMarker)] = ':'
	copy(buf[len(prefix)+1+len(SetTypeMarker)+1:], []byte(member))
	return buf
}

type MapData struct {
	Prefix string
	Count  uint64
}

func (md *MapData) Marshal() ([]byte, error) {
	buf := make([]byte, 8+len(md.Prefix))
	binary.BigEndian.PutUint64(buf[0:8], md.Count)
	copy(buf[8:], []byte(md.Prefix))
	return buf, nil
}

func UnmarshalDataFrameMapData(data []byte) (*MapData, error) {
	if len(data) < 8 {
		return nil, &DataFrameError{Op: "UnmarshalDataFrameMapData", Type: TypeMap, Msg: "data too short"}
	}

	md := &MapData{}
	md.Count = binary.BigEndian.Uint64(data[0:8])
	md.Prefix = string(data[8:])
	return md, nil
}

func (df *DataFrame) SetMap(data *MapData) error {
	if data == nil {
		return &DataFrameError{
			Op:   "SetMap",
			Type: TypeMap,
			Msg:  "data cannot be nil",
		}
	}

	buf, err := data.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal map data: %w", err)
	}

	df.typ = TypeMap
	df.payload = buf

	return nil
}

func (df *DataFrame) Map() (*MapData, error) {
	if df.typ != TypeMap {
		return nil, &DataFrameError{Op: "Map", Type: df.typ, Msg: "type mismatch"}
	}

	value, err := UnmarshalDataFrameMapData(df.payload)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map data: %w", err)
	}

	return value, nil
}

const MapTypeMarker = "{:map:}"

func MakeMapEntryKey(prefix string) []byte {
	buf := make([]byte, len(prefix)+len(MapTypeMarker)+1)
	copy(buf, []byte(prefix))
	buf[len(prefix)] = ':'
	copy(buf[len(prefix)+1:], []byte(MapTypeMarker))
	return buf
}

func MakeMapItemKey(prefix string, field string) []byte {
	buf := make([]byte, len(prefix)+len(MapTypeMarker)+len(field)+2)
	copy(buf, []byte(prefix))
	buf[len(prefix)] = ':'
	copy(buf[len(prefix)+1:], []byte(MapTypeMarker))
	buf[len(prefix)+1+len(MapTypeMarker)] = ':'
	copy(buf[len(prefix)+1+len(MapTypeMarker)+1:], []byte(field))
	return buf
}

type TimeseriesData struct {
	Prefix string
}

func (td *TimeseriesData) Marshal() ([]byte, error) {
	return []byte(td.Prefix), nil
}

func UnmarshalDataFrameTimeseriesData(data []byte) (*TimeseriesData, error) {
	if len(data) < 1 {
		return nil, &DataFrameError{Op: "UnmarshalDataFrameTimeseriesData", Type: TypeTimeseries, Msg: "data too short"}
	}

	td := &TimeseriesData{}
	td.Prefix = string(data)

	return td, nil
}

func (df *DataFrame) SetTimeseries(data *TimeseriesData) error {
	if data == nil {
		return &DataFrameError{
			Op:   "SetTimeseries",
			Type: TypeTimeseries,
			Msg:  "data cannot be nil",
		}
	}

	buf, err := data.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal timeseries data: %w", err)
	}

	df.typ = TypeTimeseries
	df.payload = buf

	return nil
}

func (df *DataFrame) Timeseries() (*TimeseriesData, error) {
	if df.typ != TypeTimeseries {
		return nil, &DataFrameError{Op: "Timeseries", Type: df.typ, Msg: "type mismatch"}
	}

	value, err := UnmarshalDataFrameTimeseriesData(df.payload)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal timeseries data: %w", err)
	}

	return value, nil
}

const TimeseriesTypeMarker = "{:ts:}"

func MakeTimeseriesEntryKey(prefix string) []byte {
	buf := make([]byte, len(prefix)+len(TimeseriesTypeMarker)+1)
	copy(buf, []byte(prefix))
	buf[len(prefix)] = ':'
	copy(buf[len(prefix)+1:], []byte(TimeseriesTypeMarker))
	return buf
}

func MakeTimeseriesDataPointKey(prefix string, timestamp time.Time) []byte {
	buf := make([]byte, len(prefix)+len(TimeseriesTypeMarker)+8+2)
	copy(buf, []byte(prefix))
	buf[len(prefix)] = ':'
	copy(buf[len(prefix)+1:], []byte(TimeseriesTypeMarker))
	buf[len(prefix)+1+len(TimeseriesTypeMarker)] = ':'
	binary.BigEndian.PutUint64(buf[len(prefix)+1+len(TimeseriesTypeMarker)+1:], uint64(timestamp.UTC().UnixNano()))
	return buf
}

type BloomFilterData struct {
	Prefix string
	Slots  int
	Salt   string
	Count  uint64
}

func (bfd *BloomFilterData) Marshal() ([]byte, error) {
	buf := make([]byte, 4+len(bfd.Salt)+1+8+len(bfd.Prefix))
	binary.BigEndian.PutUint32(buf[0:4], uint32(bfd.Slots))
	copy(buf[4:], []byte(bfd.Salt))
	buf[4+len(bfd.Salt)] = ':'
	binary.BigEndian.PutUint64(buf[4+len(bfd.Salt)+1:], bfd.Count)
	copy(buf[4+len(bfd.Salt)+1+8:], []byte(bfd.Prefix))
	return buf, nil
}

func UnmarshalDataFrameBloomFilterData(data []byte) (*BloomFilterData, error) {
	if len(data) < 4+len("bloom_salt_2025")+1+8 {
		return nil, &DataFrameError{Op: "UnmarshalDataFrameBloomFilterData", Type: TypeBloomFilter, Msg: "data too short"}
	}
	bfd := &BloomFilterData{}
	bfd.Slots = int(binary.BigEndian.Uint32(data[0:4]))
	bfd.Salt = string(data[4 : 4+len("bloom_salt_2025")])
	if data[4+len("bloom_salt_2025")] != ':' {
		return nil, &DataFrameError{Op: "UnmarshalDataFrameBloomFilterData", Type: TypeBloomFilter, Msg: "invalid separator"}
	}
	bfd.Count = binary.BigEndian.Uint64(data[4+len("bloom_salt_2025")+1 : 4+len("bloom_salt_2025")+1+8])
	bfd.Prefix = string(data[4+len("bloom_salt_2025")+1+8:])
	return bfd, nil
}

func (df *DataFrame) SetBloomFilter(data *BloomFilterData) error {
	if data == nil {
		return &DataFrameError{
			Op:   "SetBloomFilter",
			Type: TypeBloomFilter,
			Msg:  "data cannot be nil",
		}
	}

	buf, err := data.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal bloom filter data: %w", err)
	}

	df.typ = TypeBloomFilter
	df.payload = buf

	return nil
}

func (df *DataFrame) BloomFilter() (*BloomFilterData, error) {
	if df.typ != TypeBloomFilter {
		return nil, &DataFrameError{Op: "BloomFilter", Type: df.typ, Msg: "type mismatch"}
	}

	value, err := UnmarshalDataFrameBloomFilterData(df.payload)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal bloom filter data: %w", err)
	}

	return value, nil
}

const BloomFilterTypeMarker = "{:bloom:}"

func MakeBloomFilterEntryKey(prefix string) []byte {
	buf := make([]byte, len(prefix)+len(BloomFilterTypeMarker)+1)
	copy(buf, []byte(prefix))
	buf[len(prefix)] = ':'
	copy(buf[len(prefix)+1:], []byte(BloomFilterTypeMarker))
	return buf
}

func MakeBloomFilterItemKey(prefix string, item string) []byte {
	buf := make([]byte, len(prefix)+len(BloomFilterTypeMarker)+len(item)+2)
	copy(buf, []byte(prefix))
	buf[len(prefix)] = ':'
	copy(buf[len(prefix)+1:], []byte(BloomFilterTypeMarker))
	buf[len(prefix)+1+len(BloomFilterTypeMarker)] = ':'
	copy(buf[len(prefix)+1+len(BloomFilterTypeMarker)+1:], []byte(item))
	return buf
}
