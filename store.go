package gokv

import "time"

type (
	GeneratorFn func(k string, v interface{}) (Option, error)
	OptionFn    func(*Option)
	OptionFns   []OptionFn

	Option struct {
		Expired    time.Duration
		CreateTime string
	}
)

func (o OptionFns) Apply(option *Option) *Option {
	for _, f := range o {
		f(option)
	}

	return option
}

func Expired(v time.Duration) OptionFn { return func(o *Option) { o.Expired = v } }
func Apply(v Option) OptionFn          { return func(o *Option) { *o = v } }

type StoreKeys interface {
	// Keys list the keys in the store.
	Keys() ([]string, error)
}

type StoreSet interface {
	// Set stores the given value for the given key.
	// The implementation automatically marshalls the value.
	// The marshalling format depends on the implementation. It can be JSON, gob etc.
	// The key must not be "" and the value must not be nil.
	Set(k string, v interface{}, fns ...OptionFn) error
}

type StoreGet interface {
	// Get retrieves the value for the given key.
	// The implementation automatically unmarshalls the value.
	// The unmarshalling source depends on the implementation. It can be JSON, gob etc.
	// The automatic unmarshalling requires a pointer to an object of the correct type
	// being passed as parameter.
	// In case of a struct the Get method will populate the fields of the object
	// that the passed pointer points to with the values of the retrieved object's values.
	// If no value is found it returns (false, nil).
	// The key must not be "" and the pointer must not be nil.
	Get(k string, v interface{}, fn GeneratorFn) (found bool, option Option, err error)
}

type StoreDel interface {
	// Del deletes the stored value for the given key.
	// Deleting a non-existing key-value pair does NOT lead to an error.
	// The key must not be "".
	Del(k string) (found bool, err error)
}

type Closer interface {
	// Close must be called when the work with the key-value store is done.
	// Most (if not all) implementations are meant to be used long-lived,
	// so only call Close() at the very end.
	// Depending on the store implementation it might do one or more of the following:
	// Make sure all pending updates make their way to disk,
	// finish open transactions,
	// close the file handle to an embedded DB,
	// close the connection to the DB server,
	// release any open resources,
	// etc.
	// Some implementation might not need the store to be closed,
	// but as long as you work with the gokv.Store interface you never know which implementation
	// is passed to your method, so you should always call it.
	Close() error
}
