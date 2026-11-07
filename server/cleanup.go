package main

import (
	"errors"
	"io"
)

func closeListeners(listeners []io.Closer) error {
	var result error
	for i := len(listeners) - 1; i >= 0; i-- {
		result = errors.Join(result, listeners[i].Close())
	}
	return result
}
