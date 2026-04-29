package dynamodb_test

import (
	"testing"

	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/dogmatiq/persistencekit/driver/aws/dynamodb"
	"github.com/dogmatiq/persistencekit/driver/aws/dynamodb/dynamojournal"
	"github.com/dogmatiq/persistencekit/driver/aws/dynamodb/dynamokv"
	"github.com/dogmatiq/persistencekit/driver/aws/dynamodb/dynamoset"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/dynamox"
	"github.com/dogmatiq/persistencekit/internal/drivertest"
	"github.com/dogmatiq/persistencekit/internal/x/xtesting"
)

func TestNew(t *testing.T) {
	client, _ := dynamox.NewTestClient(t)
	tablePrefix := xtesting.UniqueName("new")
	cleanupTables(t, client, tablePrefix)

	d := dynamodb.New(client, tablePrefix)
	t.Cleanup(func() {
		d.Close()
	})

	drivertest.RunTests(
		t,
		d,
		dynamojournal.NewBinaryStore(client, tablePrefix+"-journal"),
		dynamokv.NewBinaryStore(client, tablePrefix+"-kv"),
		dynamoset.NewBinaryStore(client, tablePrefix+"-set"),
	)
}

func TestParseURL(t *testing.T) {
	client, endpoint := dynamox.NewTestClient(t)
	tablePrefix := xtesting.UniqueName("url")
	cleanupTables(t, client, tablePrefix)

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
		dynamojournal.NewBinaryStore(client, tablePrefix+"-journal"),
		dynamokv.NewBinaryStore(client, tablePrefix+"-kv"),
		dynamoset.NewBinaryStore(client, tablePrefix+"-set"),
	)
}

func cleanupTables(t testing.TB, client *awsdynamodb.Client, tablePrefix string) {
	t.Cleanup(func() {
		ctx := xtesting.ContextForCleanup(t)
		for _, suffix := range []string{"-journal", "-kv", "-set"} {
			if err := dynamox.DeleteTableIfExists(ctx, client, tablePrefix+suffix, nil); err != nil {
				t.Error(err)
			}
		}
	})
}
