package gorabbit

import (
	"encoding/json"
	"fmt"
)

type Marshaller interface {
	ContentType() string
	Marshal(data any) ([]byte, error)
}

type marshaller struct {
	contentType string
	marshal     func(data any) ([]byte, error)
}

func (m *marshaller) ContentType() string {
	return m.contentType
}

func (m *marshaller) Marshal(data any) ([]byte, error) {
	return m.marshal(data)
}

func NewJSONMarshaller() Marshaller {
	return &marshaller{
		contentType: "application/json",
		marshal:     json.Marshal,
	}
}

func NewTextMarshaller() Marshaller {
	return &marshaller{
		contentType: "text/plain",
		marshal: func(data any) ([]byte, error) {
			switch s := data.(type) {
			case string:
				return []byte(s), nil
			case []byte:
				return s, nil
			default:
				return nil, fmt.Errorf("cannot marshal %T as text", data)
			}
		},
	}
}
