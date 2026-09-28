# go-redis-streams

A small Go wrapper around Redis Streams consumer groups (publish, subscribe, acknowledge), built on go-redis v8.

**Status:** this is a learning project from December 2021, kept here as a code sample. It hasn't been updated since then and isn't meant for production. The rough edges are listed under [Limitations](#limitations).

## What it does

It implements a simple producer/consumer pattern on a Redis stream. You publish string payloads to a stream, read them through a consumer group as values on a Go channel, and acknowledge each one when you're done with it.

| Call | What it does in Redis |
|---|---|
| `Key(name)` | Returns `"streams:" + name`. It is only a naming helper, and no other call adds the prefix for you. |
| `(*Redis).Init()` | Reads the connection settings, creates a go-redis client (DB 0) and sends `PING`. |
| `(*Redis).Publish(stream, payload)` | `XADD stream MAXLEN 10000 * payload <payload>` |
| `(*Redis).Subscribe(stream, group, consumer, ch)` | Runs `XGROUP CREATE stream group $`, then loops on `XREADGROUP ... BLOCK 0` and sends each entry to `ch` as a `PayloadMessage`. It never returns. |
| `(*Redis).AcknowledgeMessage(stream, group, id)` | Runs `XACK`, `XTRIM stream MAXLEN 1000` and `XDEL` together in one `MULTI`/`EXEC`. |

Each value that arrives on the channel is a `PayloadMessage` (JSON tags omitted here):

```go
type PayloadMessage struct {
	Channel string                 // stream key
	GroupID string                 // consumer group
	ID      string                 // stream entry ID
	Message string                 // always "" (never populated)
	Raw     map[string]interface{} // entry fields; the published string is Raw["payload"]
}
```

## Install

```sh
go get github.com/tobhoster/go-redis-streams
```

The module path contains dashes, but the package is named `go_redis_streams`. Without an alias you refer to it as `go_redis_streams`, so it's easiest to import it with one:

```go
import streams "github.com/tobhoster/go-redis-streams"
```

The `go.mod` file declares Go 1.17. The latest tag is `v0.0.1`.

## Configuration

`Init` reads three environment variables. If there is a `.env` file in the working directory, it loads that too (via [godotenv](https://github.com/joho/godotenv)). If there isn't one, it logs `Error loading .env file` and carries on.

| Variable | Example |
|---|---|
| `REDIS_HOST` | `127.0.0.1` |
| `REDIS_PORT` | `6379` |
| `REDIS_PASSWORD` | `testpass` |

All three are required. If any of them is empty, `Init` calls `log.Fatal` and the process exits. Since the password can't be left empty, the Redis server must have one set (`requirepass`). If the server has no password, the client's `AUTH` fails, so every Redis command fails. `Publish` drops the error silently, and `Subscribe` and `AcknowledgeMessage` only log it (`Subscribe` in a tight loop).

## Usage

```go
package main

import (
	"fmt"
	"time"

	streams "github.com/tobhoster/go-redis-streams"
)

func main() {
	var rs streams.Redis
	rs.Init() // reads REDIS_HOST, REDIS_PORT and REDIS_PASSWORD (or a .env file)

	key := streams.Key("orders") // "streams:orders"

	// Subscribe does not create the stream, so publish once first. On the very
	// first run this entry is added before the group exists, so the group skips it.
	rs.Publish(key, `{"order":0}`)

	// Subscribe loops forever and sends every entry it reads into msgs.
	msgs := make(chan streams.PayloadMessage)
	go rs.Subscribe(key, "billing", "worker-1", msgs)

	go func() {
		for i := 1; i <= 3; i++ {
			time.Sleep(500 * time.Millisecond) // Subscribe has no ready signal
			rs.Publish(key, fmt.Sprintf(`{"order":%d}`, i))
		}
	}()

	for i := 0; i < 3; i++ {
		msg := <-msgs
		fmt.Println(msg.ID, msg.Raw["payload"])
		rs.AcknowledgeMessage(msg.Channel, msg.GroupID, msg.ID)
	}
}
```

On an empty Redis, the first run prints orders 1 to 3. On later runs the group already exists, so `{"order":0}` is delivered first and the output is orders 0 to 2. The loop stops after three messages, before order 3 is published.

[`example/hi.go`](example/hi.go) does the same round trip as the test below, using the same stream (`streams:testing`), group (`FOLLOW_TESTING`) and consumer (`TESTING`). It has the same first-run problem, and because it has no timeout, on an empty Redis it blocks forever. To avoid that, create the `FOLLOW_TESTING` group first (see [Running the tests](#running-the-tests)) or run it a second time.

## Running the tests

[`redis_test.go`](redis_test.go) contains one integration test, `TestRedis_Publish`. It checks that `Key` adds the `streams:` prefix. It then publishes a payload, subscribes, waits for that payload to come back and acknowledges it. The test needs a running Redis with a password. There are no tests that run without Redis.

With podman (docker works the same way):

```sh
podman run --rm -d --name redis-streams-test -p 6379:6379 \
  docker.io/library/redis:7 redis-server --requirepass testpass

# Create the test's consumer group before the first run (see below).
podman exec redis-streams-test redis-cli -a testpass \
  XGROUP CREATE streams:testing FOLLOW_TESTING '$' MKSTREAM

REDIS_HOST=127.0.0.1 REDIS_PORT=6379 REDIS_PASSWORD=testpass \
  go test -v -count=1 -timeout 60s ./...

podman stop redis-streams-test
```

The `XGROUP CREATE` step is needed because the test publishes before it subscribes, and `Subscribe` creates the group at `$` (the end of the stream). On an empty Redis, that means the group is created after the test message has already been added. The test never receives the message, and `go test` hangs until the timeout. You can avoid this by creating the group beforehand, or by running the test a second time against the same Redis.

## Limitations

- **Old dependency.** The package is built on `github.com/go-redis/redis/v8` v8.11.4. That client has since moved to `github.com/redis/go-redis/v9`, and this project hasn't been migrated.
- **Errors are mostly swallowed.** None of the methods return an error. `Publish` ignores the `XADD` result completely, so its failures are never reported. `Subscribe` and `AcknowledgeMessage` only log errors. If `PING` fails, `Init` returns an empty `*Redis` with a nil client and logs nothing. The value you called `Init` on still holds its client, so the failure only shows up later, when `Subscribe` or `AcknowledgeMessage` log failed calls. If you use the pointer `Init` returns instead (for example `r := (&streams.Redis{}).Init()`), a failed `PING` leaves you with a nil client, and the next method call panics with a nil pointer dereference.
- **No way to stop a subscriber.** `Subscribe` uses a package-level `context.Background()`. It never returns and never closes the channel.
- **No backoff.** `Subscribe` has no backoff of its own. If Redis rejects `XREADGROUP` (the password is wrong, or the group was never created), `Subscribe` logs the error and retries straight away, producing thousands of log lines per second. If Redis is unreachable, go-redis's own retry backoff slows this to roughly a dozen lines per second, but it still never stops.
- **Group creation.** The group is created at `$` without `MKSTREAM`. The stream therefore has to exist already, and any entries already in it when the group is first created are skipped.
- **Pending entries are re-delivered.** At startup, `Subscribe` reads the consumer's pending entries from `0-0`, but it never moves the read position forward. Each pending entry is sent again on every loop until it is acknowledged. In testing, even an entry acknowledged right away arrived twice.
- **Acknowledging deletes.** `AcknowledgeMessage` also runs `XDEL` on the entry and trims the stream to its newest 1000 entries. In effect, the stream works as a work queue for a single group. Other groups on the same stream can lose entries, and unread entries older than the newest 1000 are dropped.
- **Loose ends.**
  - `PayloadMessage.Message` is never set.
  - The exported `Streams` interface doesn't match the methods on `Redis`, so `*Redis` doesn't implement it, and nothing uses it.
  - Settings come only from environment variables, with no options for choosing a DB, setting an ACL username or using TLS.
  - In the test, the branch for a mismatched payload calls `t.Failed()`, which only reports status, where it should call `t.Fail()`.
