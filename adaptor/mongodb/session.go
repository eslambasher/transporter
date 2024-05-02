package mongodb

import (
	"context"
	"time"

	"github.com/compose/transporter/client"
	"go.mongodb.org/mongo-driver/mongo"
)

// Session serves as a wrapper for the underlying mgo.Session
type Session struct {
	client *mongo.Client
	dbname string
}

var _ client.Session = &Session{}

// Close implements necessary calls to cleanup the underlying mgo.Session
func (c *Session) Close() {

	if c.client != nil {
		// Create a context specifically for the disconnection process
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel() // Ensure that resources are released when the operation completes

		// Disconnect the client and clean up connection resources
		c.client.Disconnect(ctx)
	}
}
