# DBFlow Local Agent

A lightweight binary (~8 MB) that runs on your machine and lets the **DBFlow** web app connect to databases that are only accessible from localhost — such as Docker containers, local dev databases, or databases behind a firewall.

## How it works

```
Browser (DBFlow web app)
  └─► https://app.dbflow.com
        └─► POST /api/proxy/db-connections/:id/plain-params  (backend — HTTPS, authenticated)
              └─► returns decrypted credentials
  └─► fetch http://localhost:27182/introspect  (local agent on YOUR machine)
```

Credentials never leave your machine unencrypted. The agent only listens on `127.0.0.1` — it is not reachable from the network.

---

## Supported databases

| DBMS | Status | Notes |
|------|--------|-------|
| PostgreSQL | ✅ | |
| MySQL / MariaDB | ✅ | |
| SQL Server | ✅ | |
| SQLite | ✅ | `database` field = file path, e.g. `/home/user/app.db` |

---

## Quickstart

### Download a pre-built binary

Go to the [Releases](../../releases) page and download the binary for your platform:

| File | Platform |
|------|----------|
| `dbflow-agent-darwin-arm64` | macOS Apple Silicon (M1/M2/M3) |
| `dbflow-agent-darwin-amd64` | macOS Intel |
| `dbflow-agent-windows-amd64.exe` | Windows 64-bit |
| `dbflow-agent-linux-amd64` | Linux 64-bit |

**macOS / Linux:**
```bash
chmod +x dbflow-agent-darwin-arm64   # make executable
./dbflow-agent-darwin-arm64
```

**Windows:**
```powershell
.\dbflow-agent-windows-amd64.exe
```

The agent starts on `http://127.0.0.1:27182`. Keep the terminal open while using DBFlow.

---

## Build from source

**Requirements:** [Go 1.22+](https://go.dev/dl/)

```bash
git clone https://github.com/your-org/dbflow-agent
cd dbflow-agent

# Install deps
go mod tidy


# Run locally (dev)
go run .

# Build for current platform
go build -o dbflow-agent .

# Cross-compile for all platforms (output goes to ./dist/)
make all
```

`make all` produces:

```
dist/
  dbflow-agent-darwin-amd64
  dbflow-agent-darwin-arm64
  dbflow-agent-linux-amd64
  dbflow-agent-windows-amd64.exe
```

---

## API reference

All endpoints accept and return `application/json`. The agent only binds to `127.0.0.1` — never exposed to the network.

### `GET /health`

Check if the agent is running.

```json
{ "status": "ok", "agent": "dbflow-local-agent", "version": "1.0.0" }
```

---

### `POST /test`

Test a database connection.

**Request:**
```json
{
  "dbms": "postgresql",
  "host": "localhost",
  "port": 5432,
  "database": "mydb",
  "username": "postgres",
  "password": "secret",
  "ssl": false
}
```

**Response:**
```json
{ "success": true, "message": "Connection successful", "latencyMs": 12 }
```

---

### `POST /schemas`

List available schemas in a database.

**Request:** same as `/test`

**Response:**
```json
["public", "analytics", "reporting"]
```

---

### `POST /introspect`

Introspect tables, columns, and foreign keys from a schema.

**Request:**
```json
{
  "dbms": "postgresql",
  "host": "localhost",
  "port": 5432,
  "database": "mydb",
  "username": "postgres",
  "password": "secret",
  "ssl": false,
  "schema": "public"
}
```

**Response:**
```json
[
  {
    "name": "users",
    "columns": [
      { "name": "id", "dataType": "INTEGER", "nullable": false, "isPrimaryKey": true, "isUnique": true, "autoIncrement": true }
    ],
    "foreignKeys": [],
    "indexes": []
  }
]
```

---

## Security

- Binds **only** to `127.0.0.1` — not accessible from other machines on the network.
- Credentials are forwarded from the DBFlow backend (over HTTPS) and only used in-memory during the request; they are never stored by the agent.
- No authentication on the agent itself (it's localhost-only). Do not expose port `27182` in any firewall or port-forwarding rule.

---

## License

MIT
