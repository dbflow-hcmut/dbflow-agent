// DBFlow Local Agent
// ==================
// Runs on http://localhost:27182
//
// Build:
//
//	go build -o dbflow-agent .
//
// Cross-compile (see Makefile):
//
//	make all
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/microsoft/go-mssqldb"
)

const (
	agentPort    = 27182
	agentVersion = "1.0.0"
)

const asciiLogo = `
 ██████╗ ██████╗ ███████╗██╗      ██████╗ ██╗    ██╗
 ██╔══██╗██╔══██╗██╔════╝██║     ██╔═══██╗██║    ██║
 ██║  ██║██████╔╝█████╗  ██║     ██║   ██║██║ █╗ ██║
 ██║  ██║██╔══██╗██╔══╝  ██║     ██║   ██║██║███╗██║
 ██████╔╝██████╔╝██║     ███████╗╚██████╔╝╚███╔███╔╝
 ╚═════╝ ╚═════╝ ╚═╝     ╚══════╝ ╚═════╝  ╚══╝╚══╝

             Local Agent  v%s
             Listening on http://127.0.0.1:%d
`

var logger *slog.Logger

// ─── Models ──────────────────────────────────────────────────────────

type ConnectionParams struct {
	DBMS     string  `json:"dbms"`
	Host     string  `json:"host"`
	Port     *int    `json:"port"`
	Database string  `json:"database"`
	Username *string `json:"username"`
	Password *string `json:"password"`
	SSL      bool    `json:"ssl"`
}

// IntrospectParams embeds ConnectionParams — JSON decoder handles all fields.
type IntrospectParams struct {
	ConnectionParams
	Schema *string `json:"schema"`
}

type TestResult struct {
	Success   bool   `json:"success"`
	Message   string `json:"message"`
	LatencyMs int64  `json:"latencyMs,omitempty"`
}

type Column struct {
	Name          string  `json:"name"`
	DataType      string  `json:"dataType"`
	Length        *string `json:"length"`
	Nullable      bool    `json:"nullable"`
	IsPrimaryKey  bool    `json:"isPrimaryKey"`
	IsUnique      bool    `json:"isUnique"`
	AutoIncrement bool    `json:"autoIncrement"`
	DefaultValue  *string `json:"defaultValue"`
}

type ForeignKey struct {
	ConstraintName string   `json:"constraintName"`
	Columns        []string `json:"columns"`
	RefTable       string   `json:"refTable"`
	RefColumns     []string `json:"refColumns"`
	OnDelete       string   `json:"onDelete"`
	OnUpdate       string   `json:"onUpdate"`
}

type Table struct {
	Name        string        `json:"name"`
	Columns     []Column      `json:"columns"`
	ForeignKeys []ForeignKey  `json:"foreignKeys"`
	Indexes     []interface{} `json:"indexes"`
}

// ─── HTTP helpers ─────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// loggingMiddleware wraps each request with structured access logging.
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		logger.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rw.status,
			"latency_ms", time.Since(start).Milliseconds(),
		)
	})
}

// responseWriter captures the HTTP status code for logging.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func strPtr(s string) *string { return &s }

// ─── PostgreSQL ───────────────────────────────────────────────────────

func pgDSN(p ConnectionParams) string {
	port := 5432
	if p.Port != nil {
		port = *p.Port
	}
	sslmode := "disable"
	if p.SSL {
		sslmode = "require"
	}
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s connect_timeout=10",
		p.Host, port, p.Database, deref(p.Username), deref(p.Password), sslmode,
	)
}

func pgOpen(p ConnectionParams) (*sql.DB, error) {
	db, err := sql.Open("postgres", pgDSN(p))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func testPostgres(p ConnectionParams) error {
	db, err := pgOpen(p)
	if err != nil {
		return err
	}
	defer db.Close()
	logger.Debug("postgres: running SELECT 1", "host", p.Host, "db", p.Database)
	return db.QueryRow("SELECT 1").Scan(new(int))
}

func listSchemasPostgres(p ConnectionParams) ([]string, error) {
	db, err := pgOpen(p)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(`
		SELECT schema_name FROM information_schema.schemata
		WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
		  AND schema_name NOT LIKE 'pg_temp_%'
		  AND schema_name NOT LIKE 'pg_toast_temp_%'
		ORDER BY schema_name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schemas := make([]string, 0)
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		schemas = append(schemas, s)
	}
	return schemas, rows.Err()
}

func introspectPostgres(p IntrospectParams) ([]Table, error) {
	schemaName := "public"
	if p.Schema != nil && *p.Schema != "" {
		schemaName = *p.Schema
	}

	db, err := pgOpen(p.ConnectionParams)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// 1. Table names
	tRows, err := db.Query(`
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = $1 AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`, schemaName)
	if err != nil {
		return nil, err
	}
	var tableNames []string
	for tRows.Next() {
		var name string
		if err := tRows.Scan(&name); err != nil {
			tRows.Close()
			return nil, err
		}
		tableNames = append(tableNames, name)
	}
	tRows.Close()

	tables := make([]Table, 0, len(tableNames))

	for _, tableName := range tableNames {
		// Primary keys
		pkCols, err := queryStringSet(db, `
			SELECT kcu.column_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
			  ON tc.constraint_name = kcu.constraint_name
			 AND tc.table_schema = kcu.table_schema
			WHERE tc.table_schema = $1 AND tc.table_name = $2
			  AND tc.constraint_type = 'PRIMARY KEY'
		`, schemaName, tableName)
		if err != nil {
			return nil, err
		}

		// Unique constraints
		uniqueCols, err := queryStringSet(db, `
			SELECT kcu.column_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
			  ON tc.constraint_name = kcu.constraint_name
			 AND tc.table_schema = kcu.table_schema
			WHERE tc.table_schema = $1 AND tc.table_name = $2
			  AND tc.constraint_type = 'UNIQUE'
		`, schemaName, tableName)
		if err != nil {
			return nil, err
		}

		// Columns
		colRows, err := db.Query(`
			SELECT column_name, data_type, udt_name,
			       character_maximum_length, numeric_precision, numeric_scale,
			       is_nullable, column_default, is_identity
			FROM information_schema.columns
			WHERE table_schema = $1 AND table_name = $2
			ORDER BY ordinal_position
		`, schemaName, tableName)
		if err != nil {
			return nil, err
		}

		columns := make([]Column, 0)
		for colRows.Next() {
			var (
				colName    string
				dataType   string
				udtName    string
				charMaxLen sql.NullInt64
				numPrec    sql.NullInt64
				numScale   sql.NullInt64
				isNullable string
				colDefault sql.NullString
				isIdentity sql.NullString
			)
			if err := colRows.Scan(&colName, &dataType, &udtName, &charMaxLen,
				&numPrec, &numScale, &isNullable, &colDefault, &isIdentity); err != nil {
				colRows.Close()
				return nil, err
			}

			var length *string
			if charMaxLen.Valid {
				s := fmt.Sprintf("%d", charMaxLen.Int64)
				length = &s
			} else if numPrec.Valid && numScale.Valid {
				s := fmt.Sprintf("%d,%d", numPrec.Int64, numScale.Int64)
				length = &s
			}

			var defVal *string
			if colDefault.Valid {
				defVal = &colDefault.String
			}

			columns = append(columns, Column{
				Name:          colName,
				DataType:      mapPgType(udtName, dataType),
				Length:        length,
				Nullable:      isNullable == "YES",
				IsPrimaryKey:  pkCols[colName],
				IsUnique:      uniqueCols[colName],
				AutoIncrement: isIdentity.Valid && strings.ToUpper(isIdentity.String) == "YES",
				DefaultValue:  defVal,
			})
		}
		colRows.Close()

		// Foreign keys
		fkRows, err := db.Query(`
			SELECT tc.constraint_name, kcu.column_name,
			       ccu.table_name AS ref_table, ccu.column_name AS ref_col,
			       rc.delete_rule, rc.update_rule
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
			  ON tc.constraint_name = kcu.constraint_name
			 AND tc.table_schema = kcu.table_schema
			JOIN information_schema.constraint_column_usage ccu
			  ON tc.constraint_name = ccu.constraint_name
			 AND tc.table_schema = ccu.table_schema
			JOIN information_schema.referential_constraints rc
			  ON tc.constraint_name = rc.constraint_name
			 AND tc.table_schema = rc.constraint_schema
			WHERE tc.table_schema = $1 AND tc.table_name = $2
			  AND tc.constraint_type = 'FOREIGN KEY'
			ORDER BY tc.constraint_name, kcu.ordinal_position
		`, schemaName, tableName)
		if err != nil {
			return nil, err
		}

		fks := buildFKs(fkRows)
		fkRows.Close()

		tables = append(tables, Table{
			Name:        tableName,
			Columns:     columns,
			ForeignKeys: fks,
			Indexes:     []interface{}{},
		})
	}

	return tables, nil
}

func mapPgType(udtName, dataType string) string {
	m := map[string]string{
		"int2": "SMALLINT", "int4": "INTEGER", "int8": "BIGINT",
		"float4": "REAL", "float8": "DOUBLE PRECISION", "numeric": "NUMERIC",
		"bool": "BOOLEAN", "varchar": "VARCHAR", "bpchar": "CHAR", "text": "TEXT",
		"date": "DATE", "time": "TIME", "timetz": "TIME WITH TIME ZONE",
		"timestamp": "TIMESTAMP", "timestamptz": "TIMESTAMP WITH TIME ZONE",
		"uuid": "UUID", "json": "JSON", "jsonb": "JSONB", "bytea": "BYTEA",
		"inet": "INET", "cidr": "CIDR", "macaddr": "MACADDR", "xml": "XML",
		"money": "MONEY", "interval": "INTERVAL",
	}
	if t, ok := m[udtName]; ok {
		return t
	}
	return strings.ToUpper(dataType)
}

// ─── MySQL ────────────────────────────────────────────────────────────

func mysqlDSN(p ConnectionParams) string {
	port := 3306
	if p.Port != nil {
		port = *p.Port
	}
	tls := "false"
	if p.SSL {
		tls = "true"
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?tls=%s&timeout=10s&parseTime=true",
		deref(p.Username), deref(p.Password), p.Host, port, p.Database, tls)
}

func mysqlOpen(p ConnectionParams) (*sql.DB, error) {
	db, err := sql.Open("mysql", mysqlDSN(p))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func testMySQL(p ConnectionParams) error {
	db, err := mysqlOpen(p)
	if err != nil {
		return err
	}
	defer db.Close()
	logger.Debug("mysql: running SELECT 1", "host", p.Host, "db", p.Database)
	return db.QueryRow("SELECT 1").Scan(new(int))
}

func listSchemasMySQL(p ConnectionParams) ([]string, error) {
	db, err := mysqlOpen(p)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(`
		SELECT schema_name FROM information_schema.schemata
		WHERE schema_name NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
		ORDER BY schema_name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schemas := make([]string, 0)
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		schemas = append(schemas, s)
	}
	return schemas, rows.Err()
}

func introspectMySQL(p IntrospectParams) ([]Table, error) {
	dbName := p.Database
	if p.Schema != nil && *p.Schema != "" {
		dbName = *p.Schema
	}

	db, err := mysqlOpen(p.ConnectionParams)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// 1. Table names
	tRows, err := db.Query(`
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = ? AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`, dbName)
	if err != nil {
		return nil, err
	}
	var tableNames []string
	for tRows.Next() {
		var name string
		if err := tRows.Scan(&name); err != nil {
			tRows.Close()
			return nil, err
		}
		tableNames = append(tableNames, name)
	}
	tRows.Close()

	tables := make([]Table, 0, len(tableNames))

	for _, tableName := range tableNames {
		// Columns
		colRows, err := db.Query(`
			SELECT column_name, data_type,
			       character_maximum_length, numeric_precision, numeric_scale,
			       is_nullable, column_default, column_key, extra
			FROM information_schema.columns
			WHERE table_schema = ? AND table_name = ?
			ORDER BY ordinal_position
		`, dbName, tableName)
		if err != nil {
			return nil, err
		}

		columns := make([]Column, 0)
		for colRows.Next() {
			var (
				colName    string
				dataType   string
				charMaxLen sql.NullInt64
				numPrec    sql.NullInt64
				numScale   sql.NullInt64
				isNullable string
				colDefault sql.NullString
				colKey     string
				extra      string
			)
			if err := colRows.Scan(&colName, &dataType, &charMaxLen, &numPrec,
				&numScale, &isNullable, &colDefault, &colKey, &extra); err != nil {
				colRows.Close()
				return nil, err
			}

			var length *string
			if charMaxLen.Valid {
				s := fmt.Sprintf("%d", charMaxLen.Int64)
				length = &s
			} else if numPrec.Valid && numScale.Valid {
				s := fmt.Sprintf("%d,%d", numPrec.Int64, numScale.Int64)
				length = &s
			}

			var defVal *string
			if colDefault.Valid {
				defVal = &colDefault.String
			}

			columns = append(columns, Column{
				Name:          colName,
				DataType:      strings.ToUpper(dataType),
				Length:        length,
				Nullable:      isNullable == "YES",
				IsPrimaryKey:  colKey == "PRI",
				IsUnique:      colKey == "UNI" || colKey == "PRI",
				AutoIncrement: strings.Contains(strings.ToLower(extra), "auto_increment"),
				DefaultValue:  defVal,
			})
		}
		colRows.Close()

		// Foreign keys
		fkRows, err := db.Query(`
			SELECT kcu.constraint_name, kcu.column_name,
			       kcu.referenced_table_name, kcu.referenced_column_name,
			       rc.delete_rule, rc.update_rule
			FROM information_schema.key_column_usage kcu
			JOIN information_schema.referential_constraints rc
			  ON kcu.constraint_name = rc.constraint_name
			 AND kcu.table_schema = rc.constraint_schema
			WHERE kcu.table_schema = ? AND kcu.table_name = ?
			  AND kcu.referenced_table_name IS NOT NULL
			ORDER BY kcu.constraint_name, kcu.ordinal_position
		`, dbName, tableName)
		if err != nil {
			return nil, err
		}

		fks := buildFKs(fkRows)
		fkRows.Close()

		tables = append(tables, Table{
			Name:        tableName,
			Columns:     columns,
			ForeignKeys: fks,
			Indexes:     []interface{}{},
		})
	}

	return tables, nil
}

// ─── SQL Server ──────────────────────────────────────────────────────

func mssqlDSN(p ConnectionParams) string {
	port := 1433
	if p.Port != nil {
		port = *p.Port
	}
	u := &url.URL{
		Scheme: "sqlserver",
		User:   url.UserPassword(deref(p.Username), deref(p.Password)),
		Host:   fmt.Sprintf("%s:%d", p.Host, port),
	}
	q := url.Values{}
	q.Set("database", p.Database)
	if p.SSL {
		q.Set("encrypt", "true")
	} else {
		q.Set("encrypt", "disable")
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func mssqlOpen(p ConnectionParams) (*sql.DB, error) {
	db, err := sql.Open("sqlserver", mssqlDSN(p))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func testMSSQL(p ConnectionParams) error {
	db, err := mssqlOpen(p)
	if err != nil {
		return err
	}
	defer db.Close()
	logger.Debug("sqlserver: running SELECT 1", "host", p.Host, "db", p.Database)
	return db.QueryRow("SELECT 1").Scan(new(int))
}

func listSchemasMSSQL(p ConnectionParams) ([]string, error) {
	db, err := mssqlOpen(p)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.Query(`
		SELECT name FROM sys.schemas
		WHERE name NOT IN (
			'sys','INFORMATION_SCHEMA','guest','db_owner','db_accessadmin',
			'db_securityadmin','db_ddladmin','db_backupoperator',
			'db_datareader','db_datawriter','db_denydatareader','db_denydatawriter'
		)
		ORDER BY name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	schemas := make([]string, 0)
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		schemas = append(schemas, s)
	}
	return schemas, rows.Err()
}

func introspectMSSQL(p IntrospectParams) ([]Table, error) {
	schemaName := "dbo"
	if p.Schema != nil && *p.Schema != "" {
		schemaName = *p.Schema
	}
	db, err := mssqlOpen(p.ConnectionParams)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// 1. Table names
	tRows, err := db.Query(`
		SELECT t.name FROM sys.tables t
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = @p1
		ORDER BY t.name
	`, schemaName)
	if err != nil {
		return nil, err
	}
	var tableNames []string
	for tRows.Next() {
		var name string
		if err := tRows.Scan(&name); err != nil {
			tRows.Close()
			return nil, err
		}
		tableNames = append(tableNames, name)
	}
	tRows.Close()

	tables := make([]Table, 0, len(tableNames))
	for _, tableName := range tableNames {
		// Primary keys
		pkCols, err := queryStringSet(db, `
			SELECT c.name
			FROM sys.indexes i
			JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
			JOIN sys.columns c        ON ic.object_id = c.object_id AND ic.column_id = c.column_id
			JOIN sys.objects o        ON i.object_id = o.object_id
			JOIN sys.schemas s        ON o.schema_id = s.schema_id
			WHERE s.name = @p1 AND o.name = @p2 AND i.is_primary_key = 1
		`, schemaName, tableName)
		if err != nil {
			return nil, err
		}
		// Unique columns
		uniqueCols, err := queryStringSet(db, `
			SELECT c.name
			FROM sys.indexes i
			JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
			JOIN sys.columns c        ON ic.object_id = c.object_id AND ic.column_id = c.column_id
			JOIN sys.objects o        ON i.object_id = o.object_id
			JOIN sys.schemas s        ON o.schema_id = s.schema_id
			WHERE s.name = @p1 AND o.name = @p2
			  AND i.is_unique = 1 AND i.is_primary_key = 0
		`, schemaName, tableName)
		if err != nil {
			return nil, err
		}
		// Columns
		colRows, err := db.Query(`
			SELECT
				c.name,
				tp.name        AS type_name,
				c.max_length,
				c.precision,
				c.scale,
				c.is_nullable,
				c.is_identity,
				def.definition AS default_def
			FROM sys.columns c
			JOIN sys.objects o  ON c.object_id = o.object_id
			JOIN sys.schemas s  ON o.schema_id = s.schema_id
			JOIN sys.types tp   ON c.user_type_id = tp.user_type_id
			LEFT JOIN sys.default_constraints def ON c.default_object_id = def.object_id
			WHERE s.name = @p1 AND o.name = @p2 AND o.type = 'U'
			ORDER BY c.column_id
		`, schemaName, tableName)
		if err != nil {
			return nil, err
		}
		type rawColMS struct {
			name       string
			typeName   string
			maxLen     int64
			precision  int64
			scale      int64
			isNullable bool
			isIdentity bool
			defDef     sql.NullString
		}
		var rawCols []rawColMS
		for colRows.Next() {
			var r rawColMS
			if err := colRows.Scan(&r.name, &r.typeName, &r.maxLen, &r.precision, &r.scale,
				&r.isNullable, &r.isIdentity, &r.defDef); err != nil {
				colRows.Close()
				return nil, err
			}
			rawCols = append(rawCols, r)
		}
		colRows.Close()

		columns := make([]Column, 0, len(rawCols))
		for _, r := range rawCols {
			length := mssqlLength(r.typeName, r.maxLen, r.precision, r.scale)
			var defVal *string
			if r.defDef.Valid {
				defVal = &r.defDef.String
			}
			columns = append(columns, Column{
				Name:          r.name,
				DataType:      strings.ToUpper(r.typeName),
				Length:        length,
				Nullable:      r.isNullable,
				IsPrimaryKey:  pkCols[r.name],
				IsUnique:      uniqueCols[r.name],
				AutoIncrement: r.isIdentity,
				DefaultValue:  defVal,
			})
		}
		// Foreign keys
		fkRows, err := db.Query(`
			SELECT
				fk.name                                                       AS constraint_name,
				COL_NAME(fkc.parent_object_id, fkc.parent_column_id)         AS col_name,
				OBJECT_NAME(fkc.referenced_object_id)                        AS ref_table,
				COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS ref_col,
				fk.delete_referential_action_desc                            AS delete_rule,
				fk.update_referential_action_desc                            AS update_rule
			FROM sys.foreign_keys fk
			JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
			JOIN sys.objects o ON fk.parent_object_id = o.object_id
			JOIN sys.schemas s ON o.schema_id = s.schema_id
			WHERE s.name = @p1 AND o.name = @p2
			ORDER BY fk.name, fkc.constraint_column_id
		`, schemaName, tableName)
		if err != nil {
			return nil, err
		}
		fks := buildFKs(fkRows)
		fkRows.Close()

		tables = append(tables, Table{
			Name:        tableName,
			Columns:     columns,
			ForeignKeys: fks,
			Indexes:     []interface{}{},
		})
	}
	return tables, nil
}

// mssqlLength converts SQL Server sys.columns size fields to a display string.
func mssqlLength(typeName string, maxLen, precision, scale int64) *string {
	switch strings.ToUpper(typeName) {
	case "VARCHAR", "CHAR", "BINARY", "VARBINARY":
		if maxLen == -1 {
			s := "MAX"
			return &s
		}
		s := fmt.Sprintf("%d", maxLen)
		return &s
	case "NVARCHAR", "NCHAR":
		if maxLen == -1 {
			s := "MAX"
			return &s
		}
		s := fmt.Sprintf("%d", maxLen/2)
		return &s
	case "DECIMAL", "NUMERIC":
		s := fmt.Sprintf("%d,%d", precision, scale)
		return &s
	}
	return nil
}

// ─── Shared helpers ───────────────────────────────────────────────────

func queryStringSet(db *sql.DB, query string, args ...interface{}) (map[string]bool, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]bool{}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		result[s] = true
	}
	return result, rows.Err()
}

func buildFKs(rows *sql.Rows) []ForeignKey {
	fkMap := map[string]*ForeignKey{}
	fkOrder := []string{}
	for rows.Next() {
		var cname, col, refTable, refCol, delRule, updRule string
		if err := rows.Scan(&cname, &col, &refTable, &refCol, &delRule, &updRule); err != nil {
			continue
		}
		if _, exists := fkMap[cname]; !exists {
			fkMap[cname] = &ForeignKey{
				ConstraintName: cname,
				Columns:        []string{},
				RefTable:       refTable,
				RefColumns:     []string{},
				OnDelete:       delRule,
				OnUpdate:       updRule,
			}
			fkOrder = append(fkOrder, cname)
		}
		fkMap[cname].Columns = append(fkMap[cname].Columns, col)
		fkMap[cname].RefColumns = append(fkMap[cname].RefColumns, refCol)
	}
	fks := make([]ForeignKey, 0, len(fkOrder))
	for _, k := range fkOrder {
		fks = append(fks, *fkMap[k])
	}
	return fks
}

// ─── Handlers ─────────────────────────────────────────────────────────

func handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"agent":   "dbflow-local-agent",
		"version": agentVersion,
	})
}

func handleTest(w http.ResponseWriter, r *http.Request) {
	var params ConnectionParams
	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	logger.Info("test connection", "dbms", params.DBMS, "host", params.Host, "db", params.Database)

	start := time.Now()
	var testErr error

	switch params.DBMS {
	case "postgresql":
		testErr = testPostgres(params)
	case "mysql":
		testErr = testMySQL(params)
	case "sqlserver":
		testErr = testMSSQL(params)
	default:
		writeJSON(w, http.StatusOK, TestResult{
			Success: false,
			Message: fmt.Sprintf("Unsupported DBMS: %s", params.DBMS),
		})
		return
	}

	latency := time.Since(start).Milliseconds()
	if testErr != nil {
		logger.Warn("test connection failed", "dbms", params.DBMS, "host", params.Host, "error", testErr.Error())
		writeJSON(w, http.StatusOK, TestResult{
			Success:   false,
			Message:   testErr.Error(),
			LatencyMs: latency,
		})
		return
	}
	logger.Info("test connection OK", "dbms", params.DBMS, "host", params.Host, "latency_ms", latency)
	writeJSON(w, http.StatusOK, TestResult{
		Success:   true,
		Message:   "Connection successful",
		LatencyMs: latency,
	})
}

func handleSchemas(w http.ResponseWriter, r *http.Request) {
	var params ConnectionParams
	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	logger.Info("list schemas", "dbms", params.DBMS, "host", params.Host, "db", params.Database)

	var (
		schemas []string
		err     error
	)
	switch params.DBMS {
	case "postgresql":
		schemas, err = listSchemasPostgres(params)
	case "mysql":
		schemas, err = listSchemasMySQL(params)
	case "sqlserver":
		schemas, err = listSchemasMSSQL(params)
	default:
		writeJSON(w, http.StatusOK, []string{})
		return
	}

	if err != nil {
		logger.Error("list schemas failed", "dbms", params.DBMS, "host", params.Host, "error", err.Error())
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	logger.Info("schemas listed", "count", len(schemas))
	if schemas == nil {
		schemas = []string{}
	}
	writeJSON(w, http.StatusOK, schemas)
}

func handleIntrospect(w http.ResponseWriter, r *http.Request) {
	var params IntrospectParams
	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	schema := ""
	if params.Schema != nil {
		schema = *params.Schema
	}
	logger.Info("introspect", "dbms", params.DBMS, "host", params.Host, "db", params.Database, "schema", schema)

	var (
		tables []Table
		err    error
	)
	switch params.DBMS {
	case "postgresql":
		tables, err = introspectPostgres(params)
	case "mysql":
		tables, err = introspectMySQL(params)
	case "sqlserver":
		tables, err = introspectMSSQL(params)
	default:
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Unsupported DBMS: %s", params.DBMS))
		return
	}

	if err != nil {
		logger.Error("introspect failed", "dbms", params.DBMS, "host", params.Host, "error", err.Error())
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	logger.Info("introspect complete", "tables", len(tables))
	writeJSON(w, http.StatusOK, tables)
}

// ─── Main ─────────────────────────────────────────────────────────────

func main() {
	// Structured logger — outputs JSON-like key=value lines to stdout
	logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// ASCII logo banner
	fmt.Printf(asciiLogo, agentVersion, agentPort)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("POST /test", handleTest)
	mux.HandleFunc("POST /schemas", handleSchemas)
	mux.HandleFunc("POST /introspect", handleIntrospect)

	addr := fmt.Sprintf("127.0.0.1:%d", agentPort)

	logger.Info("agent started",
		"version", agentVersion,
		"address", fmt.Sprintf("http://%s", addr),
		"supported_dbms", "postgresql, mysql, sqlserver",
	)
	logger.Info("endpoints ready",
		"GET", "/health",
		"POST", "/test | /schemas | /introspect",
	)
	logger.Info("press Ctrl+C to stop")

	srv := &http.Server{
		Addr:         addr,
		Handler:      loggingMiddleware(corsMiddleware(mux)),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	if err := srv.ListenAndServe(); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
