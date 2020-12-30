package sqlc

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go.uber.org/multierr"
	"log"
	"sync"
	"text/template"
	"time"
)

type Config struct {
	DriverName     string
	DataSourceName string

	KeysSQL   string
	GetSQL    string
	SetSQL    string
	DeleteSQL string

	// RefreshInterval will Refresh the key values from the database in every Refresh interval.
	RefreshInterval time.Duration
}

// Client is a gokv.Store implementation for SQL databases.
type Client struct {
	Config

	cache     map[string]CacheValue
	cacheLock sync.Mutex
}

func DefaultDuration(s, defaultValue time.Duration) time.Duration {
	if s == 0 {
		return defaultValue
	}
	return s
}
func Default(s, defaultValue string) string {
	if s == "" {
		return defaultValue
	}

	return s
}

func NewClient(c Config) *Client {
	c.RefreshInterval = DefaultDuration(c.RefreshInterval, 60*time.Second)
	c.DriverName = Default(c.DriverName, "mysql")
	c.KeysSQL = Default(c.KeysSQL, "select k from kv where state = 1")
	c.GetSQL = Default(c.GetSQL, "select v from kv where k = '{{.Key}}' and state = 1")
	c.SetSQL = Default(c.SetSQL, "update kv set v = '{{.Value}}', updated = '{{.Time}}' where k = '{{.Key}}' and state = 1")
	c.DeleteSQL = Default(c.DeleteSQL, "update kv set state = 0, updated = '{{.Time}}' where k = '{{.Key}}' and state = 1")

	client := &Client{
		Config: c,
		cache:  make(map[string]CacheValue),
	}

	go client.tickerRefresh()

	return client
}

// CacheValue is a holder for value and option associated with a key.
type CacheValue struct {
	Value      string
	UpdateTime time.Time
}

var (
	// ErrTooManyValues is the error to identify more than one values associated with a key.
	ErrTooManyValues = errors.New("more than one values associated with the key")
)

func (c *Client) tickerRefresh() {
	ticker := time.NewTicker(c.RefreshInterval)
	for range ticker.C {
		if err := c.Refresh(); err != nil {
			log.Printf("W! refersh error %v", err)
		}
	}
}

func (c *Client) Refresh() error {
	keys, err := c.Keys()
	if err != nil {
		return err
	}

	keysMap := map[string]bool{}
	for _, k := range keys {
		keysMap[k] = true
	}

	cacheKeys := make([]string, 0)

	c.cacheLock.Lock()
	for k := range c.cache {
		if !keysMap[k] {
			delete(c.cache, k)
		} else {
			cacheKeys = append(cacheKeys, k)
		}
	}
	c.cacheLock.Unlock()

	for _, k := range cacheKeys {
		c.cacheLock.Lock()
		delete(c.cache, k)
		c.cacheLock.Unlock()

		if _, _, err := c.Get(k); err != nil {
			return err
		}
	}

	return nil
}

// Keys list the keys in the store.
func (c *Client) Keys() (keys []string, er error) {
	t, err := template.New("").Parse(c.KeysSQL)
	if err != nil {
		return nil, err
	}

	var out bytes.Buffer
	if err := t.Execute(&out, map[string]string{}); err != nil {
		return nil, err
	}
	query := out.String()
	log.Printf("D! query: %s", query)

	db, err := sql.Open(c.DriverName, c.DataSourceName)
	if err != nil {
		return nil, err
	}

	defer func() { er = multierr.Append(er, db.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	cols, _ := rows.Columns()
	results := make([]string, 0)
	for row := 0; rows.Next(); row++ {
		columns := make([]string, len(cols))
		pointers := make([]interface{}, len(cols))
		for i := range columns {
			pointers[i] = &columns[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			return nil, err
		}

		results = append(results, columns[0])
	}

	return results, nil
}

// Set stores the given value for the given key.
// Values are automatically marshalled to JSON or gob (depending on the configuration).
// The key must not be "" and the value must not be nil.
func (c *Client) Set(k, v string) (er error) {
	t, err := template.New("").Parse(c.SetSQL)
	if err != nil {
		return err
	}

	var out bytes.Buffer
	if err := t.Execute(&out, map[string]string{
		"Key":   k,
		"Value": v,
		"Time":  time.Now().Format(`2006-01-02 15:04:05.000`),
	}); err != nil {
		return err
	}

	query := out.String()
	log.Printf("D! query: %s", query)

	db, err := sql.Open(c.DriverName, c.DataSourceName)
	if err != nil {
		return err
	}

	defer func() { er = multierr.Append(er, db.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if _, err := db.ExecContext(ctx, query); err != nil {
		return err
	}

	c.cacheLock.Lock()
	c.cache[k] = CacheValue{
		Value:      v,
		UpdateTime: time.Now(),
	}
	c.cacheLock.Unlock()

	return nil
}

// Get retrieves the stored value for the given key.
// You need to pass a pointer to the value, so in case of a struct
// the automatic unmarshalling can populate the fields of the object
// that v points to with the values of the retrieved object's values.
// If no value is found it returns (false, nil).
func (c *Client) Get(k string) (found bool, v string, er error) {
	c.cacheLock.Lock()
	if v, ok := c.cache[k]; ok {
		c.cacheLock.Unlock()

		return true, v.Value, nil
	}
	c.cacheLock.Unlock()

	t, err := template.New("").Parse(c.GetSQL)
	if err != nil {
		return false, "", err
	}

	var out bytes.Buffer
	if err := t.Execute(&out, map[string]string{"Key": k}); err != nil {
		return false, "", err
	}

	query := out.String()
	log.Printf("D! query: %s", query)

	db, err := sql.Open(c.DriverName, c.DataSourceName)
	if err != nil {
		return false, "", err
	}

	defer func() { er = multierr.Append(er, db.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return false, "", err
	}

	cols, _ := rows.Columns()
	row := 0

	for ; rows.Next(); row++ {
		if row >= 1 {
			return false, "", fmt.Errorf("key:%s, error:%w", k, ErrTooManyValues)
		}

		columns := make([]sql.NullString, len(cols))
		pointers := make([]interface{}, len(cols))
		for i := range columns {
			pointers[i] = &columns[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			return false, "", err
		}

		v = columns[0].String
	}

	if row == 1 {
		c.cacheLock.Lock()
		c.cache[k] = CacheValue{
			Value:      v,
			UpdateTime: time.Now(),
		}
		c.cacheLock.Unlock()

		return true, v, nil
	}

	return false, "", nil
}

// Del deletes the stored value for the given key.
// Deleting a non-existing key-value pair does NOT lead to an error.
// The key must not be "".
func (c *Client) Del(k string) (er error) {
	c.cacheLock.Lock()
	delete(c.cache, k)
	c.cacheLock.Unlock()

	defer func() {
		if err := c.del(k); err != nil {
			log.Printf("W! failed to del %v", err)
		}
	}()

	return nil

}
func (c *Client) del(k string) (er error) {
	t, err := template.New("").Parse(c.DeleteSQL)
	if err != nil {
		return err
	}

	var out bytes.Buffer
	if err := t.Execute(&out, map[string]string{
		"Key":  k,
		"Time": time.Now().Format(`2006-01-02 15:04:05.000`),
	}); err != nil {
		return err
	}

	query := out.String()
	log.Printf("D! query: %s", query)

	db, err := sql.Open(c.DriverName, c.DataSourceName)
	if err != nil {
		return err
	}

	defer func() { er = multierr.Append(er, db.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if _, err := db.ExecContext(ctx, query); err != nil {
		return err
	}

	return nil
}
