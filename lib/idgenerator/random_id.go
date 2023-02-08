package idgenerator

import "github.com/segmentio/ksuid"

func GenID() string {
	return ksuid.New().String()
}
