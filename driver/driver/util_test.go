package driver

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStructToMutationParams(t *testing.T) {
	s := struct {
		ID   int `common:"iD"`
		Name string
	}{
		ID: 11,
		Name: "4514",
	}

	cols, vals, ptrs, err := StructToMutationParams(&s)

	assert.Empty(t, err)
	assert.Equal(t, cols[0], "iD")
	assert.Equal(t, cols[1], "name")
	assert.Equal(t, vals[0].(int), 11)

	*ptrs[0].(*int) = 1919
	assert.Equal(t, s.ID, 1919)
}

func TestSlice2Str(t *testing.T) {
	strSlice := []string{"made", "in", "abyss"}
	intSlice := []int{1, 1, 4, 5, 1, 4}

	assert.Equal(t, Slice2Str(strSlice), "made,in,abyss")
	assert.Equal(t, Slice2Str(intSlice), "1,1,4,5,1,4")
}
