package sqlc_test

import (
	"fmt"
	"github.com/bingoohuang/gokv"
	"github.com/bingoohuang/gokv/pkg/codec"
	"github.com/bingoohuang/gokv/pkg/sqlc"
	_ "github.com/go-sql-driver/mysql"
	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/auth"
	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/server"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"log"
	"net"
	"testing"
)

func TestSQL(t *testing.T) {
	driver := sqle.NewDefault()
	db, err := createTestDatabase("testdb")
	assert.Nil(t, err)
	driver.AddDatabase(db)

	l, _ := net.Listen("tcp", ":0")
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()

	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("localhost:%d", port),
		Auth:     auth.NewNativeSingle("user", "pass", auth.AllPermissions),
	}

	s, err := server.NewDefaultServer(config, driver)
	assert.Nil(t, err)

	go func() {
		if err := s.Start(); err != nil {
			log.Print("start", err)
		}
	}()

	client := sqlc.Client{
		DriverName:     "mysql",
		DataSourceName: fmt.Sprintf("user:pass@tcp(localhost:%d)/testdb", port),
		KeysSQL:        "select k from kv where state = 1",
		GetSQL:         "select v, option from kv where k = '{{.Key}}' and state = 1",
		SetSQL:         "update kv set v = '{{.Value}}', updated = '{{.Time}}' where k = '{{.Key}}' and state = 1",
		DeleteSQL:      "update kv set state = 0, updated = '{{.Time}}' where k = '{{.Key}}' and state = 1",
		Codec:          codec.JSON,
	}

	assert.Nil(t, client.Set("Key1", "bingoohuang"))

	v := ""
	found, option, err := client.Get("Key1", &v, nil)
	assert.Nil(t, err)
	assert.True(t, found)
	assert.Equal(t, "bingoohuang", v)
	assert.Equal(t, gokv.Option{}, option)
}

func createTestDatabase(dbName string) (*memory.Database, error) {
	const tableName = "kv"

	db := memory.NewDatabase(dbName)
	table := memory.NewTable(tableName, sql.Schema{
		{Name: "k", Type: sql.VarChar(10), Nullable: false, Source: tableName, PrimaryKey: true},
		{Name: "v", Type: sql.Text, Nullable: false, Source: tableName},
		{Name: "option", Type: sql.Text, Nullable: true, Source: tableName},
		{Name: "state", Type: sql.Int8, Nullable: false, Source: tableName},
		{Name: "updated", Type: sql.VarChar(30), Nullable: true, Source: tableName},
		{Name: "created", Type: sql.VarChar(30), Nullable: true, Source: tableName},
	})

	db.AddTable(tableName, table)
	ctx := sql.NewEmptyContext()

	rows := []sql.Row{
		sql.NewRow("Key1", `"value1"`, nil, 1, nil, nil),
		sql.NewRow("Key2", `"value2"`, nil, 1, nil, nil),
		sql.NewRow("Key3", `"value3"`, nil, 1, nil, nil),
	}

	for _, row := range rows {
		if err := table.Insert(ctx, row); err != nil {
			return nil, err
		}
	}

	return db, nil
}
