package mongodb

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

var _ client.Writer = &Writer{}

type Writer struct {
	writeMap map[ops.Op]func(context.Context, message.Msg, *mongo.Collection) error
}

// Assume insertMsg, updateMsg, deleteMsg have the following signatures:
// insertMsg(ctx context.Context, msg message.Msg, col *mongo.Collection) error
// updateMsg(ctx context.Context, msg message.Msg, col *mongo.Collection) error
// deleteMsg(ctx context.Context, msg message.Msg, col *mongo.Collection) error

func newWriter() *Writer {
	w := &Writer{}
	w.writeMap = map[ops.Op]func(context.Context, message.Msg, *mongo.Collection) error{
		ops.Insert: insertMsg, // Ensure insertMsg accepts context.Context
		ops.Update: updateMsg, // Ensure updateMsg accepts context.Context
		ops.Delete: deleteMsg, // Ensure deleteMsg accepts context.Context
	}
	return w
}

func (w *Writer) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(s client.Session) (message.Msg, error) {
		// Initiate a context at the beginning of the operation
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		writeFunc, ok := w.writeMap[msg.OP()]
		if !ok {
			log.Infof("no function registered for operation, %s\n", msg.OP())
			if msg.Confirms() != nil {
				msg.Confirms() <- struct{}{}
			}
			return msg, nil // Or consider handling this case as an error
		}

		// Assuming msgCollection has been adapted to require a context
		collection := msgCollection(ctx, msg, s.(*Session).client)
		if collection == nil {
			// Handling error or invalid collection case appropriately
			return nil, errors.New("unable to get collection")
		}

		if err := writeFunc(ctx, msg, collection); err != nil {
			return nil, err
		}

		if msg.Confirms() != nil {
			msg.Confirms() <- struct{}{}
		}

		return msg, nil
	}
}

func msgCollection(ctx context.Context, msg message.Msg, client *mongo.Client) *mongo.Collection {
	// Extract the database and collection names from the message's namespace.
	// This example assumes the namespace is in the format "database.collection".
	namespaceParts := strings.SplitN(msg.Namespace(), ".", 2)
	if len(namespaceParts) != 2 {
		// Log error or handle it according to your application's needs.
		return nil
	}
	dbName, collectionName := namespaceParts[0], namespaceParts[1]

	// Although the context isn't used to get a collection reference,
	// it's prepared for use in subsequent operations that require it.
	return client.Database(dbName).Collection(collectionName)
}

func insertMsg(ctx context.Context, msg message.Msg, col *mongo.Collection) error {
	_, err := col.InsertOne(ctx, msg.Data())
	if mongo.IsDuplicateKeyError(err) {
		// If a duplicate key error occurred, attempt to update
		return updateMsg(ctx, msg, col)
	}
	return err
}

func updateMsg(ctx context.Context, msg message.Msg, col *mongo.Collection) error {
	filter := bson.M{"_id": msg.Data()["_id"]} // Ensure "_id" is correctly extracted from msg.Data()
	update := bson.M{"$set": msg.Data()}       // Assuming you want to replace the entire document except for _id
	_, err := col.UpdateOne(ctx, filter, update)
	return err
}

func deleteMsg(ctx context.Context, msg message.Msg, col *mongo.Collection) error {
	_, err := col.DeleteOne(ctx, bson.M{"_id": msg.Data()["_id"]}) // Ensure "_id" is correctly extracted from msg.Data()
	return err
}
