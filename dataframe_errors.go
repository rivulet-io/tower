package tower

import (
	"errors"
	"time"
)

type DataframeExpiredError struct {
	id        string
	expiredAt time.Time
}

func (e *DataframeExpiredError) Error() string {
	return "dataframe with id " + e.id + " expired at " + e.expiredAt.String()
}

func IsDataframeExpiredError(err error) *DataframeExpiredError {
	var de *DataframeExpiredError
	if errors.As(err, &de) {
		return de
	}

	return nil
}

func NewDataframeExpiredError(id string, expiredAt time.Time) error {
	return &DataframeExpiredError{
		id:        id,
		expiredAt: expiredAt,
	}
}
