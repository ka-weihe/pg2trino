package main_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/docker/go-connections/nat"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// waitForTrinoInitialization checks if Trino is fully initialized by polling the /v1/info endpoint.
func waitForTrinoInitialization(infoURL string) error {
	for i := 0; i < 60; i++ {
		resp, err := http.Get(infoURL)
		if err != nil {
			log.Printf("Waiting for Trino server to start... Retrying")
			time.Sleep(1 * time.Second)
			continue
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		var info struct {
			Starting bool `json:"starting"`
		}

		if err := json.Unmarshal(body, &info); err != nil {
			return err
		}

		if !info.Starting {
			return nil // Trino has finished starting
		}

		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("trino server did not initialize in time")
}

var _ = Describe("Trino Container", func() {
	var (
		ctx            context.Context
		trinoContainer testcontainers.Container
		db             *sql.DB
		dsn            string
		mappedPort     nat.Port
	)

	BeforeEach(func() {
		ctx = context.Background()

		// Define the Trino container request
		req := testcontainers.ContainerRequest{
			Image:        "trinodb/trino",
			ExposedPorts: []string{"8080/tcp"},
			WaitingFor:   wait.ForListeningPort("8080/tcp"),
			Env:          map[string]string{},
		}

		// Start the Trino container
		var err error
		trinoContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		Expect(err).NotTo(HaveOccurred(), "Failed to start container")

		// Get the container's mapped port
		mappedPort, err = trinoContainer.MappedPort(ctx, "8080")
		Expect(err).NotTo(HaveOccurred(), "Failed to get mapped port")

		// Build the DSN (Data Source Name)
		hostname := "localhost"
		catalog := "memory"
		schema := "default"
		dsn = fmt.Sprintf("http://user@%s?catalog=%s&schema=%s",
			net.JoinHostPort(hostname, mappedPort.Port()),
			catalog, schema,
		)

		// Open a connection to Trino
		db, err = sql.Open("trino", dsn)
		Expect(err).NotTo(HaveOccurred(), "Failed to open connection to Trino")
	})

	AfterEach(func() {
		Expect(db.Close()).To(Succeed())
		Expect(trinoContainer.Terminate(ctx)).To(Succeed())
	})

	It("should initialize the Trino server and perform operations", func() {
		// Polling the Trino server to check if it's fully initialized
		err := waitForTrinoInitialization(fmt.Sprintf("http://localhost:%s/v1/info", mappedPort.Port()))
		Expect(err).NotTo(HaveOccurred(), "Trino server initialization check failed")

		// Use db to interact with Trino, e.g., to create a schema, a table, and insert data
		_, err = db.Exec("CREATE SCHEMA IF NOT EXISTS test")
		Expect(err).NotTo(HaveOccurred(), "Failed to create schema")

		_, err = db.Exec("CREATE TABLE IF NOT EXISTS test.my_table ( " +
			"bool_col BOOLEAN, " +
			"tinyint_col TINYINT, " +
			"smallint_col SMALLINT, " +
			"int_col INT, " +
			"bigint_col BIGINT, " +
			"real_col REAL, " +
			"double_col DOUBLE, " +
			"decimal_col DECIMAL, " +
			"varchar_col VARCHAR, " +
			"char_col CHAR(4), " +
			"varbinary_col VARBINARY, " +
			"json_col JSON, " +
			"date_col DATE," +
			"time_col TIME," +
			"timep_col TIME(3)," +
			"timez_col TIME WITH TIME ZONE," +
			"timestamp_col TIMESTAMP," +
			"timestampp_col TIMESTAMP(3)," +
			"timestampz_col TIMESTAMP WITH TIME ZONE," +
			"timestampzp_col TIMESTAMP(3) WITH TIME ZONE," +
			"intervalym_col INTERVAL YEAR TO MONTH," +
			"intervalds_col INTERVAL DAY TO SECOND," +
			"array_col ARRAY<INTEGER>," +
			"map_col MAP<INTEGER, VARCHAR>" +
			")")
		Expect(err).NotTo(HaveOccurred(), "Failed to create table")

		// Insert data
		_, err = db.Exec("INSERT INTO test.my_table VALUES (" +
			"true, " +
			"1, " +
			"2, " +
			"3, " +
			"4, " +
			"5.5, " +
			"6.6, " +
			"7.7, " +
			"'varchar', " +
			"'char', " +
			"X'65683F', " +
			"JSON '{\"key\": \"value\"}', " +
			"date '2021-01-01'," +
			"time '12:00:00'," +
			"time '12:00:00.123'," +
			"time '01:02:03.456 -08:00'," +
			"timestamp '2021-01-01 12:00:00'," +
			"timestamp '2021-01-01 12:00:00.123'," +
			"timestamp '2001-08-22 03:04:05 UTC'," +
			"timestamp '2021-01-01 12:00:00.123 UTC'," +
			"interval '1' MONTH," +
			"interval '2' DAY," +
			"ARRAY[1, 2, 3]," +
			"MAP(ARRAY[1, 2, 3], ARRAY['one', 'two', 'three'])" +
			")")

		Expect(err).NotTo(HaveOccurred(), "Failed to insert data")
	})
})
