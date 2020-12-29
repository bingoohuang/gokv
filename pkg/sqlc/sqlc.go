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
	"log"
	"text/template"
	"time"
)

// Client is a gokv.Store implementation for SQL databases.
type Client struct {
	DriverName     string
	DataSourceName string

	KeysSQL   string
	GetSQL    string
	SetSQL    string
	DeleteSQL string

	Codec codec.Codec
}

var (
	ErrNoRowsAffected = errors.New("rowsAffected is 0")
	ErrTooManyValues  = errors.New("too many values associated with the key")
)

// Keys list the keys in the store.
func (c Client) Keys() ([]string, error) {
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

	defer db.Close()

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
func (c Client) Set(k string, v interface{}, fns ...gokv.OptionFn) error {
	if err := util.CheckKeyAndValue(k, v); err != nil {
		return err
	}

	// First turn the passed object into something that the SQL database can handle
	data, err := c.Codec.Marshal(v)
	if err != nil {
		return err
	}

	option := gokv.OptionFns(fns).Apply(&gokv.Option{})
	option.CreateTime = time.Now().Format(`2006-01-02 15:04:05.000`)

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
		"Value":  string(data),
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

	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err = db.ExecContext(ctx, query)
	return err
}

// Get retrieves the stored value for the given key.
// You need to pass a pointer to the value, so in case of a struct
// the automatic unmarshalling can populate the fields of the object
// that v points to with the values of the retrieved object's values.
// If no value is found it returns (false, nil).
// The key must not be "" and the pointer must not be nil.
func (c Client) Get(k string, v interface{}, fn gokv.GeneratorFn) (found bool, option gokv.Option, err error) {
	if err := util.CheckKeyAndValue(k, v); err != nil {
		return false, option, err
	}

	t, err := template.New("").Parse(c.GetSQL)
	if err != nil {
		return false, option, err
	}

	var out bytes.Buffer
	if err := t.Execute(&out, map[string]string{"Key": k}); err != nil {
		return false, option, err
	}

	query := out.String()
	log.Printf("D! query: %s", query)

	db, err := sql.Open(c.DriverName, c.DataSourceName)
	if err != nil {
		return false, option, err
	}

	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return false, option, err
	}

	cols, _ := rows.Columns()
	row := 0

	for ; rows.Next(); row++ {
		if row >= 1 {
			return false, option, fmt.Errorf("key:%s, error:%w", k, ErrTooManyValues)
		}

		columns := make([]sql.NullString, len(cols))
		pointers := make([]interface{}, len(cols))
		for i := range columns {
			pointers[i] = &columns[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			return false, option, err
		}

		if columns[0].String != "" {
			if err := c.Codec.Unmarshal([]byte(columns[0].String), v); err != nil {
				return false, option, err
			}
		}

		if len(cols) > 1 && columns[1].String != "" {
			if err := c.Codec.Unmarshal([]byte(columns[1].String), &option); err != nil {
				return false, option, err
			}
		}
	}

	if row == 0 && fn == nil {
		return false, option, nil
	} else if row == 1 {
		return true, option, nil
	}

	newOption, err := fn(k, v)
	if err != nil {
		return false, option, err
	}

	c.Set(k, v, gokv.Apply(newOption))
	return true, newOption, nil
}

// Del deletes the stored value for the given key.
// Deleting a non-existing key-value pair does NOT lead to an error.
// The key must not be "".
func (c Client) Del(k string) (found bool, err error) {
	if err := util.CheckKey(k); err != nil {
		return false, err
	}

	t, err := template.New("").Parse(c.DeleteSQL)
	if err != nil {
		return false, err
	}

	var out bytes.Buffer
	if err := t.Execute(&out, map[string]string{"Key": k}); err != nil {
		return false, err
	}

	db, err := sql.Open(c.DriverName, c.DataSourceName)
	if err != nil {
		return false, err
	}

	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	result, err := db.ExecContext(ctx, out.String())
	if err != nil {
		return false, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}

	return rowsAffected > 0, nil
}
