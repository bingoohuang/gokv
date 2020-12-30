package sqlc

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/bingoohuang/gokv"
	"github.com/bingoohuang/gokv/pkg/codec"
	"github.com/bingoohuang/gokv/pkg/util"
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

	Codec codec.Codec

	// RefreshInterval will Refresh the key values from the database in every Refresh interval.
	RefreshInterval time.Duration
}

// Client is a gokv.Store implementation for SQL databases.
type Client struct {
	Config

	Cache     map[string]CacheValue
	cacheLock sync.Mutex
}

func NewClient(c Config) *Client {
	client := &Client{
		Config: c,
		Cache:  make(map[string]CacheValue),
	}

	if client.Codec == nil {
		client.Codec = codec.JSON
	}

	if client.RefreshInterval > 0 {
		go client.tickerRefresh()
	}

	return client
}

// CacheValue is a holder for value and option associated with a key.
type CacheValue struct {
	Value      string
	Option     gokv.Option
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
	for k := range c.Cache {
		if !keysMap[k] {
			delete(c.Cache, k)
		} else {
			cacheKeys = append(cacheKeys, k)
		}
	}
	c.cacheLock.Unlock()

	for _, k := range cacheKeys {
		c.cacheLock.Lock()
		delete(c.Cache, k)
		c.cacheLock.Unlock()

		if _, _, _, err := c.Get(k, nil); err != nil {
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
func (c *Client) Set(k, v string, fns ...gokv.OptionFn) (er error) {
	if err := util.CheckKeyAndValue(k, v); err != nil {
		return err
	}

	option := gokv.OptionFns(fns).Apply(&gokv.Option{})

	optionData, err := c.Codec.Marshal(option)
	if err != nil {
		return err
	}

	t, err := template.New("").Parse(c.SetSQL)
	if err != nil {
		return err
	}

	var out bytes.Buffer
	if err := t.Execute(&out, map[string]string{
		"Key":    k,
		"Value":  v,
		"Option": string(optionData),
		"Time":   time.Now().Format(`2006-01-02 15:04:05.000`),
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
	c.Cache[k] = CacheValue{
		Value:      v,
		Option:     *option,
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
// The key must not be "" and the pointer must not be nil.
func (c *Client) Get(k string, fn gokv.GeneratorFn) (found bool, v string, option gokv.Option, er error) {
	if err := util.CheckKeyAndValue(k, v); err != nil {
		return false, "", option, err
	}

	c.cacheLock.Lock()
	if v, ok := c.Cache[k]; ok {
		c.cacheLock.Unlock()

		return true, v.Value, v.Option, nil
	}
	c.cacheLock.Unlock()

	t, err := template.New("").Parse(c.GetSQL)
	if err != nil {
		return false, "", option, err
	}

	var out bytes.Buffer
	if err := t.Execute(&out, map[string]string{"Key": k}); err != nil {
		return false, "", option, err
	}

	query := out.String()
	log.Printf("D! query: %s", query)

	db, err := sql.Open(c.DriverName, c.DataSourceName)
	if err != nil {
		return false, "", option, err
	}

	defer func() { er = multierr.Append(er, db.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return false, "", option, err
	}

	cols, _ := rows.Columns()
	row := 0

	for ; rows.Next(); row++ {
		if row >= 1 {
			return false, "", option, fmt.Errorf("key:%s, error:%w", k, ErrTooManyValues)
		}

		columns := make([]sql.NullString, len(cols))
		pointers := make([]interface{}, len(cols))
		for i := range columns {
			pointers[i] = &columns[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			return false, "", option, err
		}

		if columns[0].String != "" {
			v = columns[0].String
		}

		if len(cols) > 1 && columns[1].String != "" {
			if err := c.Codec.Unmarshal([]byte(columns[1].String), &option); err != nil {
				return false, "", option, err
			}
		}
	}

	if row == 0 && fn == nil {
		return false, v, option, nil
	} else if row == 1 {
		c.cacheLock.Lock()
		c.Cache[k] = CacheValue{
			Value:      v,
			Option:     option,
			UpdateTime: time.Now(),
		}
		c.cacheLock.Unlock()

		return true, v, option, nil
	}

	v, newOption, err := fn(k)
	if err != nil {
		return false, "", option, err
	}

	if err := c.Set(k, v, gokv.Apply(newOption)); err != nil {
		return false, "", option, err
	}

	return true, v, newOption, nil
}

// Del deletes the stored value for the given key.
// Deleting a non-existing key-value pair does NOT lead to an error.
// The key must not be "".
func (c *Client) Del(k string) (found bool, er error) {
	if err := util.CheckKey(k); err != nil {
		return false, err
	}

	t, err := template.New("").Parse(c.DeleteSQL)
	if err != nil {
		return false, err
	}

	var out bytes.Buffer
	if err := t.Execute(&out, map[string]string{
		"Key":  k,
		"Time": time.Now().Format(`2006-01-02 15:04:05.000`),
	}); err != nil {
		return false, err
	}

	query := out.String()
	log.Printf("D! query: %s", query)

	db, err := sql.Open(c.DriverName, c.DataSourceName)
	if err != nil {
		return false, err
	}

	defer func() { er = multierr.Append(er, db.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if _, err := db.ExecContext(ctx, query); err != nil {
		return false, err
	}

	c.cacheLock.Lock()
	delete(c.Cache, k)
	c.cacheLock.Unlock()

	return true, nil
}
