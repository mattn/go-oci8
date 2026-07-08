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
	connector := &Connector{
		Logger: log.New(ioutil.Discard, "", 0),
	}
	if len(hosts) > 0 {
		connector.dsnString = hosts[0]
	}
	return connector
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

	connDriver, err := Driver.Open(connector.dsnString)
	if err != nil {
		return nil, err
	}

	conn := connDriver.(*Conn)
	if connector.Logger != nil {
		conn.logger = connector.Logger
	}

	return conn, nil
}
