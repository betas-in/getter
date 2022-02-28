package getter

// Response ...
type Response struct {
	Code        int
	ContentType string
	Data        []byte
	DataPath    string
	Error       error
}
