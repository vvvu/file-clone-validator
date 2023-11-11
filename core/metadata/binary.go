package metadata

import (
	"encoding/json"
)

func Serialise(meta *Meta) ([]byte, error) {
	return json.Marshal(meta)
}

func Deserialise(data []byte) (*Meta, error) {
	meta := &Meta{}
	err := json.Unmarshal(data, meta)
	return meta, err
}
