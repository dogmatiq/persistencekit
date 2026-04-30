package dynamodb_test

import (
	"net/url"
	"testing"

	"github.com/dogmatiq/persistencekit/driver/aws/dynamodb"
	"github.com/dogmatiq/persistencekit/driver/aws/dynamodb/dynamojournal"
	"github.com/dogmatiq/persistencekit/driver/aws/dynamodb/dynamokv"
	"github.com/dogmatiq/persistencekit/driver/aws/dynamodb/dynamoset"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/dynamox"
	"github.com/dogmatiq/persistencekit/internal/drivertest"
	"github.com/dogmatiq/persistencekit/internal/x/xtesting"
)

func TestNew(t *testing.T) {
	var (
		tablePrefix  = xtesting.UniqueName("new")
		journalTable = tablePrefix + "-journal"
		kvTable      = tablePrefix + "-kv"
		setTable     = tablePrefix + "-set"
	)

	client, _ := dynamox.NewTestClient(t)
	dynamox.CleanupTable(t, client, journalTable, kvTable, setTable)

	d := dynamodb.New(client, tablePrefix)
	t.Cleanup(func() {
		d.Close()
	})

	drivertest.RunTests(
		t,
		d,
		dynamojournal.NewBinaryStore(client, journalTable),
		dynamokv.NewBinaryStore(client, kvTable),
		dynamoset.NewBinaryStore(client, setTable),
	)
}

func TestParseURL(t *testing.T) {
	var (
		tablePrefix  = xtesting.UniqueName("url")
		journalTable = tablePrefix + "-journal"
		kvTable      = tablePrefix + "-kv"
		setTable     = tablePrefix + "-set"
	)

	client, endpoint := dynamox.NewTestClient(t)
	dynamox.CleanupTable(t, client, journalTable, kvTable, setTable)

	t.Setenv("AWS_ACCESS_KEY_ID", "id")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")

	open, err := dynamodb.ParseURL("dynamodb://" + endpoint + "/" + tablePrefix + "?region=us-east-1&insecure")
	if err != nil {
		t.Fatal(err)
	}

	d, err := open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		d.Close()
	})

	drivertest.RunTests(
		t,
		d,
		dynamojournal.NewBinaryStore(client, journalTable),
		dynamokv.NewBinaryStore(client, kvTable),
		dynamoset.NewBinaryStore(client, setTable),
	)
}

func TestFromURL(t *testing.T) {
	t.Run("it returns a working driver", func(t *testing.T) {
		var (
			tablePrefix  = xtesting.UniqueName("fromurl")
			journalTable = tablePrefix + "-journal"
			kvTable      = tablePrefix + "-kv"
			setTable     = tablePrefix + "-set"
		)

		client, endpoint := dynamox.NewTestClient(t)
		dynamox.CleanupTable(t, client, journalTable, kvTable, setTable)

		t.Setenv("AWS_ACCESS_KEY_ID", "id")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")

		u := &url.URL{Scheme: "dynamodb", Host: endpoint, Path: "/" + tablePrefix, RawQuery: "region=us-east-1&insecure"}
		open, err := dynamodb.FromURL(u)
		if err != nil {
			t.Fatal(err)
		}

		d, err := open(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			d.Close()
		})

		drivertest.RunTests(
			t,
			d,
			dynamojournal.NewBinaryStore(client, journalTable),
			dynamokv.NewBinaryStore(client, kvTable),
			dynamoset.NewBinaryStore(client, setTable),
		)
	})

	t.Run("when the URL is invalid", func(t *testing.T) {
		cases := []struct {
			Name string
			URL  *url.URL
		}{
			{"wrong scheme", &url.URL{Scheme: "other", Path: "/prefix"}},
			{"empty table prefix", &url.URL{Scheme: "dynamodb"}},
			{"insecure without host", &url.URL{Scheme: "dynamodb", Path: "/prefix", RawQuery: "insecure"}},
			{"unknown parameter", &url.URL{Scheme: "dynamodb", Path: "/prefix", RawQuery: "unknown=value"}},
		}
		for _, tc := range cases {
			t.Run(tc.Name, func(t *testing.T) {
				_, err := dynamodb.FromURL(tc.URL)
				if err == nil {
					t.Fatal("expected an error")
				}
			})
		}
	})
}
