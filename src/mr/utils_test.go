package mr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClearFile(t *testing.T) {
	err := DelFileByMapId(10, "/home/xwd/Course/MIT68540/6.5840/src/main")
	assert.Nil(t, err)
}
