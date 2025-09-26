package op

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
)

// CreateTimeSeries creates a new time series.
func (op *Operator) CreateTimeSeries(key string) error {
	unlock := op.lock(key)
	defer unlock()

	// Check if the time series already exists
	if _, err := op.get(key); err == nil {
		return fmt.Errorf("time series %s already exists", key)
	}

	// Create the time series metadata
	tsData := &TimeseriesData{
		Prefix: key,
	}

	df := NULLDataFrame()
	err := df.SetTimeseries(tsData)
	if err != nil {
		return fmt.Errorf("failed to create timeseries data: %w", err)
	}

	// Store the time series metadata
	err = op.set(key, df)
	if err != nil {
		return fmt.Errorf("failed to store timeseries: %w", err)
	}

	return nil
}

// DeleteTimeSeries deletes an entire time series and all its data points.
func (op *Operator) DeleteTimeSeries(key string) error {
	unlock := op.lock(key)
	defer unlock()

	return op.deleteTimeSeries(key)
}

func (op *Operator) deleteTimeSeries(key string) error {
	// Check if the time series exists
	if _, err := op.get(key); err != nil {
		return fmt.Errorf("time series %s does not exist", key)
	}

	// For now, just delete the metadata
	// TODO: Delete all data points in batch
	return op.db.Delete([]byte(key), &pebble.WriteOptions{Sync: false})
}

// ExistsTimeSeries checks if a time series exists.
func (op *Operator) ExistsTimeSeries(key string) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	_, err := op.get(key)
	if err == nil {
		return true, nil
	}
	return false, nil
}

// AddTimeSeriesPoint adds a data point to a time series at the specified timestamp.
func (op *Operator) AddTimeSeriesPoint(key string, timestamp time.Time, value PrimitiveData) error {
	unlock := op.lock(key)
	defer unlock()

	// Check if the time series exists
	if _, err := op.get(key); err != nil {
		return fmt.Errorf("time series %s does not exist", key)
	}

	// Create the data point key
	dataPointKey := MakeTimeseriesDataPointKey(key, timestamp)

	// Convert PrimitiveData to DataFrame
	df := NULLDataFrame()
	switch value.Type() {
	case TypeInt:
		intVal, _ := value.Int()
		if err := df.SetInt(intVal); err != nil {
			return fmt.Errorf("failed to set int value: %w", err)
		}
	case TypeFloat:
		floatVal, _ := value.Float()
		if err := df.SetFloat(floatVal); err != nil {
			return fmt.Errorf("failed to set float value: %w", err)
		}
	case TypeString:
		strVal, _ := value.String()
		if err := df.SetString(strVal); err != nil {
			return fmt.Errorf("failed to set string value: %w", err)
		}
	case TypeBool:
		boolVal, _ := value.Bool()
		if err := df.SetBool(boolVal); err != nil {
			return fmt.Errorf("failed to set bool value: %w", err)
		}
	case TypeTimestamp:
		timeVal, _ := value.Time()
		if err := df.SetTimestamp(timeVal); err != nil {
			return fmt.Errorf("failed to set timestamp value: %w", err)
		}
	case TypeDuration:
		durVal, _ := value.Duration()
		if err := df.SetDuration(durVal); err != nil {
			return fmt.Errorf("failed to set duration value: %w", err)
		}
	case TypeBinary:
		binVal, _ := value.Binary()
		if err := df.SetBinary(binVal); err != nil {
			return fmt.Errorf("failed to set binary value: %w", err)
		}
	default:
		return fmt.Errorf("unsupported data type: %v", value.Type())
	}

	valueBytes, err := df.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal dataframe: %w", err)
	}

	// Store the data point
	err = op.db.Set(dataPointKey, valueBytes, &pebble.WriteOptions{Sync: false})
	if err != nil {
		return fmt.Errorf("failed to store data point: %w", err)
	}

	return nil
}

// GetTimeSeriesPoint retrieves a data point from a time series at the specified timestamp.
func (op *Operator) GetTimeSeriesPoint(key string, timestamp time.Time) (PrimitiveData, error) {
	unlock := op.lock(key)
	defer unlock()

	// Check if the time series exists
	if _, err := op.get(key); err != nil {
		return nil, fmt.Errorf("time series %s does not exist", key)
	}

	// Create the data point key
	dataPointKey := MakeTimeseriesDataPointKey(key, timestamp)

	// Get the data point
	value, closer, err := op.db.Get(dataPointKey)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, fmt.Errorf("data point does not exist")
		}
		return nil, fmt.Errorf("failed to get data point: %w", err)
	}
	defer closer.Close()

	// Unmarshal the value
	df, err := UnmarshalDataFrame(value)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal dataframe: %w", err)
	}

	// Convert DataFrame to PrimitiveData
	switch df.Type() {
	case TypeInt:
		intVal, _ := df.Int()
		return PrimitiveInt(intVal), nil
	case TypeFloat:
		floatVal, _ := df.Float()
		return PrimitiveFloat(floatVal), nil
	case TypeString:
		strVal, _ := df.String()
		return PrimitiveString(strVal), nil
	case TypeBool:
		boolVal, _ := df.Bool()
		return PrimitiveBool(boolVal), nil
	case TypeTimestamp:
		timeVal, _ := df.Timestamp()
		return PrimitiveTime(timeVal), nil
	case TypeDuration:
		durVal, _ := df.Duration()
		return PrimitiveDuration(durVal), nil
	case TypeBinary:
		binVal, _ := df.Binary()
		return PrimitiveBinary(binVal), nil
	default:
		return nil, fmt.Errorf("unsupported data type: %v", df.Type())
	}
}

// DeleteTimeSeriesPoint removes a data point from a time series at the specified timestamp.
func (op *Operator) DeleteTimeSeriesPoint(key string, timestamp time.Time) error {
	unlock := op.lock(key)
	defer unlock()

	// Check if the time series exists
	if _, err := op.get(key); err != nil {
		return fmt.Errorf("time series %s does not exist", key)
	}

	// Create the data point key
	dataPointKey := MakeTimeseriesDataPointKey(key, timestamp)

	// Check if the data point exists
	_, closer, err := op.db.Get(dataPointKey)
	if err != nil {
		if err == pebble.ErrNotFound {
			return fmt.Errorf("data point does not exist")
		}
		return fmt.Errorf("failed to check data point: %w", err)
	}
	defer closer.Close()

	// Remove the data point
	err = op.db.Delete(dataPointKey, &pebble.WriteOptions{Sync: false})
	if err != nil {
		return fmt.Errorf("failed to delete data point: %w", err)
	}

	return nil
}

// GetTimeSeriesRange retrieves all data points in a time series within the specified time range.
func (op *Operator) GetTimeSeriesRange(key string, startTime, endTime time.Time) (map[time.Time]PrimitiveData, error) {
	unlock := op.lock(key)
	defer unlock()

	// Check if the time series exists
	if _, err := op.get(key); err != nil {
		return nil, fmt.Errorf("time series %s does not exist", key)
	}

	result := make(map[time.Time]PrimitiveData)

	// Create iteration bounds - use a broader range to capture all time series data points
	keyPrefix := fmt.Sprintf("%s:%s:", key, TimeseriesTypeMarker)
	lowerBound := []byte(keyPrefix)
	upperBound := append([]byte(keyPrefix), 0xff)

	iter, err := op.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		// Extract timestamp from key
		keyBytes := iter.Key()
		prefixLen := len(keyPrefix)
		if len(keyBytes) >= prefixLen+8 {
			timestampNanos := int64(binary.BigEndian.Uint64(keyBytes[prefixLen:]))
			timestamp := time.Unix(0, timestampNanos).UTC()

			// Check if timestamp is within range
			if timestamp.Before(startTime) || timestamp.After(endTime) {
				continue
			}

			// Unmarshal the value
			df, err := UnmarshalDataFrame(iter.Value())
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal dataframe: %w", err)
			}

			// Convert DataFrame to PrimitiveData
			switch df.Type() {
			case TypeInt:
				intVal, _ := df.Int()
				result[timestamp] = PrimitiveInt(intVal)
			case TypeFloat:
				floatVal, _ := df.Float()
				result[timestamp] = PrimitiveFloat(floatVal)
			case TypeString:
				strVal, _ := df.String()
				result[timestamp] = PrimitiveString(strVal)
			case TypeBool:
				boolVal, _ := df.Bool()
				result[timestamp] = PrimitiveBool(boolVal)
			case TypeTimestamp:
				timeVal, _ := df.Timestamp()
				result[timestamp] = PrimitiveTime(timeVal)
			case TypeDuration:
				durVal, _ := df.Duration()
				result[timestamp] = PrimitiveDuration(durVal)
			case TypeBinary:
				binVal, _ := df.Binary()
				result[timestamp] = PrimitiveBinary(binVal)
			}
		}
	}

	return result, nil
}
