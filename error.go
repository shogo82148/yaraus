package yaraus

// Error is yaraus error.
type Error struct {
	Err         string
	ClientID    string
	ID          uint
	IsInvalidID bool
}

func (err *Error) Error() string {
	return err.Err
}

// InvalidID returns err.ID is invalid.
// If it is true, the client MUST NOT use the id anymore.
func (err *Error) InvalidID() bool {
	return err.IsInvalidID
}

// InvalidID is an interface for validating ids.
type InvalidID interface {
	InvalidID() bool
}

const invalidErrorSentinel = "YARAUS_INVALID_ID"
