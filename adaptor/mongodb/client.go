package mongodb

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	DefaultURI               = "mongodb://127.0.0.1:27017/test"
	DefaultSessionTimeout    = 10 * time.Second
	DefaultMaxWriteBatchSize = 1000
)

var (
	// DefaultSafety is the default saftey mode used for the underlying session.
	// These default settings are only good for local use as it makes not guarantees for writes.
	DefaultSafety = mgo.Safe{}

	_ client.Client = &Client{}
	_ client.Closer = &Client{}
)

// OplogAccessError wraps the underlying error when access to the oplog fails.
type OplogAccessError struct {
	reason string
}

func (e OplogAccessError) Error() string {
	return fmt.Sprintf("oplog access failed, %s", e.reason)
}

// InvalidReadPreferenceError represents the error when an incorrect mongo read preference has been set.
type InvalidReadPreferenceError struct {
	ReadPreference string
}

func (e InvalidReadPreferenceError) Error() string {
	return fmt.Sprintf("Invalid Read Preference, %s", e.ReadPreference)
}

func extractDBNameFromURI(uri string) (string, error) {
	// Splitting the URI to extract the database name part
	parts := strings.Split(uri, "/")
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid MongoDB URI: %s", uri)
	}
	dbname := parts[len(parts)-1]
	// Further split if query parameters exist
	if strings.Contains(dbname, "?") {
		dbname = strings.Split(dbname, "?")[0]
	}
	return dbname, nil
}

// ClientOptionFunc is a function that configures a Client.
// It is used in NewClient.
type ClientOptionFunc func(*Client) error

// Client represents a client to the underlying MongoDB source
type Client struct {
	uri               string
	tlsConfig         *tls.Config
	sessionTimeout    time.Duration
	tail              bool
	readPreference    *readpref.ReadPref
	maxWriteBatchSize int

	client        *mongo.Client // This replaces mgoSession
	dbname        string
	clientOptions *options.ClientOptions
}

// NewClient creates a new client to work with MongoDB.
//
// The caller can configure the new client by passing configuration options
// to the func.
//
// Example:
//
//	client, err := NewClient(
//	  WithURI("mongodb://localhost:27017"),
//	  WithTimeout("30s"))
//
// If no URI is configured, it uses defaultURI by default.
//
// An error is also returned when some configuration option is invalid
func NewClient(options ...ClientOptionFunc) (*Client, error) {
	// Set up the client
	c := &Client{
		uri:               DefaultURI,
		tlsConfig:         nil,
		tail:              false,
		readPreference:    readpref.Primary(),
		maxWriteBatchSize: DefaultMaxWriteBatchSize,
		dbname:            "",
	}

	// Run the options on it
	for _, option := range options {
		if err := option(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// WithURI defines the full connection string of the MongoDB database.
func WithURI(uri string) ClientOptionFunc {
	return func(c *Client) error {
		// In this refactored version, you don't need to explicitly parse the URI for validation.
		// The MongoDB Go Driver will validate the URI when you attempt to connect.
		// However, you can still perform some basic checks if needed (e.g., non-empty string).

		if uri == "" {
			return errors.New("URI cannot be empty")
		}

		if dbname, err := extractDBNameFromURI(uri); err == nil {
			c.dbname = dbname
		} else {
			c.dbname = ""
		}

		// Assuming c.clientOptions is an instance of *options.ClientOptions
		if c.clientOptions == nil {
			c.clientOptions = options.Client() // Initialize if not already done
		}

		c.uri = uri

		// Set the URI directly on the client options.
		// The MongoDB Go driver takes care of parsing and using this URI when connecting.
		c.clientOptions.ApplyURI(uri)

		return nil
	}
}

// WithSSL configures the database connection to connect via TLS. Setting ssl to true
// without proper TLS configuration will enable SSL with certificate verification skipped,
// which should be used with caution.
func WithSSL(ssl bool, insecureSkipVerify bool) ClientOptionFunc {
	return func(c *Client) error {
		if ssl {
			tlsConfig := &tls.Config{}

			// Control whether to perform SSL cert verification.
			tlsConfig.InsecureSkipVerify = insecureSkipVerify

			if !insecureSkipVerify {
				// Assuming you want to use the system's root CA set when not skipping verification.
				// This is important for validating server certificates.
				tlsConfig.RootCAs = x509.NewCertPool()
			}

			// Make sure to initialize clientOptions if it hasn't been already
			if c.clientOptions == nil {
				c.clientOptions = options.Client()
			}

			// Apply the TLS configuration for the MongoDB client
			c.clientOptions.SetTLSConfig(tlsConfig)
		}
		return nil
	}
}

func WithCACerts(certs []string) ClientOptionFunc {
	return func(c *Client) error {
		if len(certs) > 0 {
			roots := x509.NewCertPool()
			for _, certPath := range certs {
				certBytes, err := ioutil.ReadFile(certPath)
				if err != nil {
					return errors.New("could not read cert file: " + certPath)
				}

				if ok := roots.AppendCertsFromPEM(certBytes); !ok {
					return errors.New("failed to append cert from PEM")
				}
			}
			if c.clientOptions == nil {
				c.clientOptions = options.Client()
			}
			// Ensure any preset TLS config is respected or create a new one
			tlsConfig := c.clientOptions.TLSConfig
			if tlsConfig == nil {
				tlsConfig = &tls.Config{}
			}
			tlsConfig.RootCAs = roots
			tlsConfig.InsecureSkipVerify = false // Set based on your security policies

			// Apply the TLS config to the client options
			c.clientOptions.SetTLSConfig(tlsConfig)
		}
		return nil
	}
}

// WithWriteConcern configures the write concern option for the MongoDB client.
func WithWriteConcern(wc int) ClientOptionFunc {
	return func(c *Client) error {
		// If wc is less than 1, it implies a default or unspecified write concern.
		// MongoDB's default write concern is applied in such cases.
		// The default behavior is server-defined, typically equivalent to acknowledging
		// the write operation on the primary only (w: 1).
		writeConcern := writeconcern.New(writeconcern.W(wc))
		if wc > 0 {
			if c.clientOptions == nil {
				c.clientOptions = options.Client() // Initialize if not already done
			}
			c.clientOptions.SetWriteConcern(writeConcern)
		}
		return nil
	}
}

// WithTail set the flag to tell the Client whether or not access to the oplog will be
// needed (Default: false).
func WithTail(tail bool) ClientOptionFunc {
	return func(c *Client) error {
		c.tail = tail
		return nil
	}
}

// WithReadPreference sets the MongoDB read preference based on the provided string.
// WithReadPreference sets the MongoDB read preference based on the provided string.
func WithReadPreference(readPreference string) ClientOptionFunc {
	return func(c *Client) error {
		var rp *readpref.ReadPref

		switch strings.ToLower(readPreference) {
		case "primary":
			rp = readpref.Primary()
		case "primarypreferred":
			rp = readpref.PrimaryPreferred()
		case "secondary":
			rp = readpref.Secondary()
		case "secondarypreferred":
			rp = readpref.SecondaryPreferred()
		case "nearest":
			rp = readpref.Nearest()
		default:
			//return InvalidReadPreferenceError{ReadPreference: readPreference}
			rp = readpref.Primary()
		}

		// Assuming c.clientOptions is an instance of *options.ClientOptions that you're building up
		if c.clientOptions == nil {
			c.clientOptions = options.Client()
		}
		c.clientOptions.SetReadPreference(rp)

		return nil
	}
}

func (c *Client) Connect() (client.Session, error) {

	// If the client has already been initialized, perform a quick ping to verify the connection is still good.
	if c.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second) // Short timeout for ping
		defer cancel()
		if err := c.client.Ping(ctx, nil); err != nil {
			// If the ping fails, you may choose to log this error, handle it, or attempt to reconnect.
			// For this example, let's log and attempt reconnection.
			log.Infoln("Ping to MongoDB failed, attempting to reconnect: %s", err)
			c.client = nil // Reset client before reconnection
			// Note: Depending on your application's requirements, you might handle this differently.
		} else {
			// Connection is still active
			return &Session{c.client, c.dbname}, nil
		}
	}

	// Create a context with timeout for initializing connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Attempt to initialize the connection if it has not been done or needs to be redone.
	if c.client == nil {
		var err error
		err = c.initConnection(ctx)
		if err != nil {
			return nil, err
		}
	}

	// Perform a ping to ensure the newly established connection is active.
	if err := c.client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	return &Session{c.client, c.dbname}, nil
}

func (c *Client) Close() {
	if c.client != nil {
		// Create a context specifically for the disconnection process
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel() // Ensure that resources are released when the operation completes

		// Disconnect the client and clean up connection resources
		err := c.client.Disconnect(ctx)
		if err != nil {
			log.Errorln("Error on closing connection")
		}
	}
}

func (c *Client) initConnection(ctx context.Context) error {
	// Configuration for MongoDB client
	clientOptions := options.Client().ApplyURI(c.uri)
	log.With("database", c.dbname).Infof("connecting to URI %s", c.uri)

	// log client options

	if c.tlsConfig != nil {
		clientOptions = clientOptions.SetTLSConfig(c.tlsConfig)
	}

	// Connecting to MongoDB
	mongoClient, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	// Perform a ping to validate connectivity
	if err := mongoClient.Ping(ctx, nil); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %v", err)
	}

	// Check for oplog access if 'tail' is enabled
	if c.tail {
		if err := c.checkOplogAccess(ctx, mongoClient); err != nil {
			return err // Directly returning the error from checkOplogAccess
		}
	}

	c.client = mongoClient
	return nil
}

// checkOplogAccess checks whether access to the oplog collection is available.
func (c *Client) checkOplogAccess(ctx context.Context, mongoClient *mongo.Client) error {
	database := mongoClient.Database("local")
	collections, err := database.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to list collections in 'local' database: %v", err)
	}

	oplogFound := false
	for _, collName := range collections {
		if collName == "oplog.rs" {
			oplogFound = true
			break
		}
	}

	if !oplogFound {
		return fmt.Errorf("database missing 'oplog.rs' collection")
	}

	// Further checks for Oplog read access could go here, using queries etc.,
	// but MongoDB's roles and permissions typically manage this.

	return nil
}
