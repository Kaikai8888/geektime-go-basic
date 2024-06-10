package util

import (
	"github.com/google/uuid"
)

type UuidFn func() (string, error)

func NewUuidFn() UuidFn {
	return func() (string, error) {
		id, err := uuid.NewRandom()
		if err != nil {
			return "", err
		}

		idStr := id.String()
		return idStr, nil
	}
}
