package util

import (
	"errors"
)

// CheckKeyAndValue returns an error if k == "" or if v == nil
func CheckKeyAndValue(k string, v interface{}) error {
	if err := CheckKey(k); err != nil {
		return err
	}

	return CheckVal(v)
}

var ErrEmptyKey = errors.New("key is empty")

// CheckKey returns an error if k == ""
func CheckKey(k string) error {
	if k == "" {
		return ErrEmptyKey
	}

	return nil
}

// CheckVal returns an error if v == nil
func CheckVal(v interface{}) error {
	return nil
}
