package avro_test

import (
	"sync"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/require"
)

func TestConcurrentParse(t *testing.T) {
	var wg sync.WaitGroup
	const numThreads = 50000

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := avro.Parse(`{ "type": "record", "name": "Envelope", "namespace": "changefeed.pg.testtenant_teststore_abc_kv.public.kvs", "fields": [ { "name": "before", "type": [ "null", { "type": "record", "name": "Value", "fields": [ { "name": "tenant", "type": "string" }, { "name": "store", "type": "string" }, { "name": "partition_key", "type": "string" }, { "name": "sort_key", "type": "string" }, { "name": "version", "type": "string" }, { "name": "updated_at", "type": { "type": "string", "connect.version": 1, "connect.name": "io.debezium.time.ZonedTimestamp" } }, { "name": "tags", "type": [ "null", { "type": "array", "items": [ "null", "string" ] } ], "default": null }, { "name": "data", "type": [ "null", "string" ], "default": null }, { "name": "encoding", "type": [ "null", "string" ], "default": null }, { "name": "schema", "type": [ "null", "long" ], "default": null }, { "name": "lookup_keys", "type": [ "null", { "type": "array", "items": [ "null", "string" ] } ], "default": null } ], "connect.name": "changefeed.pg.testtenant_teststore_abc_kv.public.kvs.Value" } ], "default": null }, { "name": "after", "type": [ "null", "Value" ], "default": null }, { "name": "source", "type": { "type": "record", "name": "Source", "namespace": "io.debezium.connector.postgresql", "fields": [ { "name": "version", "type": "string" }, { "name": "connector", "type": "string" }, { "name": "name", "type": "string" }, { "name": "ts_ms", "type": "long" }, { "name": "snapshot", "type": [ { "type": "string", "connect.version": 1, "connect.parameters": { "allowed": "true,last,false,incremental" }, "connect.default": "false", "connect.name": "io.debezium.data.Enum" }, "null" ], "default": "false" }, { "name": "db", "type": "string" }, { "name": "sequence", "type": [ "null", "string" ], "default": null }, { "name": "ts_us", "type": [ "null", "long" ], "default": null }, { "name": "ts_ns", "type": [ "null", "long" ], "default": null }, { "name": "schema", "type": "string" }, { "name": "table", "type": "string" }, { "name": "txId", "type": [ "null", "long" ], "default": null }, { "name": "lsn", "type": [ "null", "long" ], "default": null }, { "name": "xmin", "type": [ "null", "long" ], "default": null } ], "connect.name": "io.debezium.connector.postgresql.Source" } }, { "name": "transaction", "type": [ "null", { "type": "record", "name": "block", "namespace": "event", "fields": [ { "name": "id", "type": "string" }, { "name": "total_order", "type": "long" }, { "name": "data_collection_order", "type": "long" } ], "connect.version": 1, "connect.name": "event.block" } ], "default": null }, { "name": "op", "type": "string" }, { "name": "ts_ms", "type": [ "null", "long" ], "default": null }, { "name": "ts_us", "type": [ "null", "long" ], "default": null }, { "name": "ts_ns", "type": [ "null", "long" ], "default": null } ], "connect.version": 2, "connect.name": "changefeed.pg.testtenant_teststore_abc_kv.public.kvs.Envelope" }`)
			require.NoError(t, err)
		}()
	}

	wg.Wait()
}
