// +build go1.10

package oci8

import (
	"context"
	"database/sql/driver"
	"io/ioutil"
	"log"
)

// NewConnector returns a new database connector
func NewConnector(hosts ...string) driver.Connector {
	return &OCI8Connector{
		Logger: log.New(ioutil.Discard, "", 0),
	}
}

// Driver returns the OCI8 driver
func (oci8Connector *OCI8Connector) Driver() driver.Driver {
	return OCI8Driver
}

// Connect returns a new database connection
func (oci8Connector *OCI8Connector) Connect(ctx context.Context) (driver.Conn, error) {
	oci8Conn := &OCI8Conn{
		logger: oci8Connector.Logger,
	}
	if oci8Conn.logger == nil {
		oci8Conn.logger = log.New(ioutil.Discard, "", 0)
	}

	return oci8Conn, nil
}
