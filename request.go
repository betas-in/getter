package getter

import (
	"net/url"
	"time"
)

// Request ...
type Request struct {
	Timeout       time.Duration
	Method        string
	Path          string
	Query         map[string]string
	Headers       map[string]string
	SendFormData  bool
	FormData      url.Values
	FormDataMap   map[string]string
	SendJSONData  bool
	JSONData      []byte
	JSONInterface interface{}
	SaveToDisk    bool
}
