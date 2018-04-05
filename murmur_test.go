package reconcile

import (
	"testing"
)

func TestSum128x32(t *testing.T) {
	tests := []struct {
		title string
		data  []byte
		out   [4]uint32
	}{
		{
			"Match one block",
			[]byte("This is 16 bytes"),
			[...]uint32{0xd42f6b0a, 0x9a95f367, 0xdcd64279, 0x98f8e6d5},
		},
		{
			"Match two blocks",
			[]byte("This is 32 bytes 'cuz we need it"),
			[...]uint32{0x63ea548f, 0x4c2ed36e, 0xba490a09, 0xedbb8a10},
		},
		{
			"Match with 15 byte tail",
			[]byte("This is 47 bytes so we can have a 15-byte tail."),
			[...]uint32{0x373e6102, 0x3309e580, 0x5babab6c, 0x35d0b798},
		},
	}

	for _, test := range tests {
		actual := Sum128x32(test.data, 0)
		if actual != test.out {
			t.Error(
				"For", test.title, "test",
				"expected", test.out,
				"got", actual)
		}
	}
}
