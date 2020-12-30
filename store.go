package gokv

import "time"

type (
	GeneratorFn func(k string) (string, Option, error)
	OptionFn    func(*Option)
	OptionFns   []OptionFn

	Option struct {
		Expired time.Time
	}
)

func (o OptionFns) Apply(option *Option) *Option {
	for _, f := range o {
		f(option)
	}

	return option
}

func Expired(v time.Duration) OptionFn { return func(o *Option) { o.Expired = time.Now().Add(v) } }
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
	Set(k, v string, fns ...OptionFn) error
}

type StoreGet interface {
	// Get retrieves the value for the given key.
	Get(k string, fn GeneratorFn) (found bool, v string, option Option, err error)
}

type StoreDel interface {
	// Del deletes the stored value for the given key.
	// Deleting a non-existing key-value pair does NOT lead to an error.
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
