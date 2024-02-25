package kvraft

import (
	"sync"

	"github.com/google/uuid"
)

var uuidLock sync.Mutex

func newUUID() string {
	uuidLock.Lock()
	defer uuidLock.Unlock()
	return uuid.NewString()
}
