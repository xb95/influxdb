/*
 * OpenTSDB ingestion API
 *
 * Provides a service that translates OpenTSDB style metric reports to the
 * appropriate InfluxDB system.
 *
 * For more information on OpenTSDB: http://opentsdb.net/
 *
 */

package opentsdb

import (
	"bufio"
	"cluster"
	log "code.google.com/p/log4go"
	. "common"
	"coordinator"
	"net"
	"protocol"
	"strconv"
	"strings"
)

type Server struct {
	closed        bool
	listen_addr   string
	database      string
	coordinator   coordinator.Coordinator
	clusterConfig *cluster.ClusterConfiguration
	conn          net.Listener
	user          *cluster.ClusterAdmin
}

type OpenTSDBListener interface {
	Close()
	getAuth()
	ListenAndServe()
	writePoints(protocol.Series) error
}

// TODO: check that database exists and create it if not
func NewServer(listen_addr, database string, coord coordinator.Coordinator,
	clusterConfig *cluster.ClusterConfiguration) *Server {
	// Create new server object
	return &Server{
		listen_addr:   listen_addr,
		database:      database,
		coordinator:   coord,
		clusterConfig: clusterConfig,
	}
}

// getAuth assures that the user property is a user with access to the given database.
// Only call this function after everything (i.e. Raft) is initialized, so that there's
// at least one admin user.
//
// TODO: There should be a saner way of doing this; at the moment, since the OpenTSDB
// protocol does not support authentication, we just grab the first admin user and use
// that.
func (self *Server) getAuth() {
	names := self.clusterConfig.GetClusterAdmins()
	self.user = self.clusterConfig.GetClusterAdmin(names[0])
}

// ListenAndServe starts up a TCP socket and waits for connections.
func (self *Server) ListenAndServe() {
	self.getAuth()
	if self.listen_addr != "" {
		var err error
		if self.conn, err = net.Listen("tcp", self.listen_addr); err != nil {
			log.Error("OpenTSDBServer: Listen: ", err)
			return
		}
	}

	for !self.closed {
		conn_in, err := self.conn.Accept()
		if err != nil {
			log.Error("OpenTSDBServer: accept: %s", err)
			continue
		}
		go self.handleClient(conn_in)
	}
}

// Close is invoked when we need to shut down the server. This tries to ensure any in-flight
// requests are allowed to finish.
func (self *Server) Close() {
	if self.closed {
		return
	}

	log.Info("OpenTSDBServer: stopping listener")
	self.conn.Close()

	// TODO: Is there a race here? Who calls Close()? If they're in a different goroutine
	// then we could possibly be causing problems?
	self.closed = true

	// TODO: Figure out if we can/should close outstanding clients?
}

// handleClient is run in a goroutine, one per connected endpoint. This reads in lines
// in the OpenTSDB format and converts them to InfluxDB series.
//
// OpenTSDB format: put proc.loadavg.1m 1288946927 0.36 host=foo
func (self *Server) handleClient(conn_in net.Conn) {
	defer conn_in.Close()
	scanner := bufio.NewScanner(conn_in)
	for scanner.Scan() {
		fields := strings.Fields(string(scanner.Bytes()))
		if len(fields) < 4 || fields[0] != "put" {
			continue
		}

		// OpenTSDB presently uses second-precision 32 bit unsigned timestamps
		timestamp, err := strconv.ParseUint(fields[2], 10, 32)
		if err != nil {
			continue
		}

		// Values in OpenTSDB are defined as floating point
		val, err := strconv.ParseFloat(fields[3], 64)
		if err != nil {
			continue
		}

		// Any extra fields will be additional columns with values
		// TODO: implement

		// Conversion to InfluxDB formats
		values := []*protocol.FieldValue{}
		if i := int64(val); float64(i) == val {
			values = append(values, &protocol.FieldValue{Int64Value: &i})
		} else {
			values = append(values, &protocol.FieldValue{DoubleValue: &val})
		}

		// Microsecond precision, signed time conversion
		ts := int64(timestamp * 1000000)

		// Use same SN makes sure that we'll only keep the latest value for a given
		// metric_id-timestamp pair
		sn := uint64(1)

		point := &protocol.Point{
			Timestamp:      &ts,
			Values:         values,
			SequenceNumber: &sn,
		}

		series := &protocol.Series{
			Name:   &fields[1],
			Fields: []string{"value"},
			Points: []*protocol.Point{point},
		}

		// This is a little inefficient for now, later we might want to add multiple
		// series in 1 writePoints request
		self.writePoints(series)
	}
	if err := scanner.Err(); err != nil {
		log.Error("OpenTSDBServer: failed reading: %s", err)
	}
}

// writePoints attempts to write out data to InfluxDB and handles authorization errors
// and other internal issues.
func (self *Server) writePoints(series *protocol.Series) error {
	err := self.coordinator.WriteSeriesData(self.user, self.database, series)
	if err != nil {
		switch err.(type) {
		case AuthorizationError:
			// user information got stale, get a fresh one (this should happen rarely)
			self.getAuth()
			err = self.coordinator.WriteSeriesData(self.user, self.database, series)
			if err != nil {
				log.Warn("OpenTSDBServer: failed to write series after new auth: %s", err)
			}
		default:
			log.Warn("OpenTSDBServer: failed write series: %s", err)
		}
	}
	return err
}
