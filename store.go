package gokv

type Store interface {
	// Keys list the keys in the store.
	All() (map[string]string, error)
	// Set stores the given value for the given key.
	Set(k, v string) error
	// Get retrieves the value for the given key.
	Get(k string) (v string, err error)
	// Del deletes the stored value for the given key.
	// Deleting a non-existing key-value pair does NOT lead to an error.
	Del(k string) error
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
