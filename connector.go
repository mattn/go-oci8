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
	return &Connector{
		Logger: log.New(ioutil.Discard, "", 0),
	}
}

// Driver returns the OCI8 driver
func (connector *Connector) Driver() driver.Driver {
	return Driver
}

// Connect returns a new database connection
func (connector *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	conn := &Conn{
		logger: connector.Logger,
	}
	if conn.logger == nil {
		conn.logger = log.New(ioutil.Discard, "", 0)
	}

	return conn, nil
}
