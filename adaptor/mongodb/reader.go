package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/commitlog"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/data"
	"github.com/compose/transporter/message/ops"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/mgo.v2"
)

var (
	_ client.Reader = &Reader{}

	// DefaultCollectionFilter is an empty map of empty maps
	DefaultCollectionFilter = map[string]CollectionFilter{}
)

// CollectionFilter is just a typed map of strings of map[string]interface{}
type CollectionFilter map[string]interface{}

// Reader implements the behavior defined by client.Reader for interfacing with MongoDB.
type Reader struct {
	tail              bool
	collectionFilters map[string]CollectionFilter
	oplogTimeout      time.Duration
}

func newReader(tail bool, filters map[string]CollectionFilter) client.Reader {
	return &Reader{tail, filters, 5 * time.Second}
}

type resultDoc struct {
	doc bson.M
	c   string
}

type iterationComplete struct {
	oplogTime primitive.Timestamp
	c         string
}

func (r *Reader) Read(resumeMap map[string]client.MessageSet, filterFn client.NsFilterFunc) client.MessageChanFunc {
	return func(s client.Session, done chan struct{}) (chan client.MessageSet, error) {
		out := make(chan client.MessageSet)
		client := s.(*Session).client

		go func() {

			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()

			defer func() {
				close(out)
			}()
			var dbname = s.(*Session).dbname
			log.With("db", dbname).Infoln("starting Read func")
			collections, err := r.listCollections(ctx, client, dbname, filterFn)
			if err != nil {
				log.With("db", dbname).Errorf("unable to list collections, %s", err)
				return
			}
			var wg sync.WaitGroup
			for _, collectionName := range collections {
				var lastID interface{}
				oplogTime := timeAsMongoTimestamp(time.Now())
				var mode commitlog.Mode // default to Copy
				if m, ok := resumeMap[collectionName]; ok {
					lastID = m.Msg.Data().Get("_id")
					mode = m.Mode
					oplogTime = timeAsMongoTimestamp(time.Unix(m.Timestamp, 0))
				}
				if mode == commitlog.Copy {
					if err := r.iterateCollection(r.iterate(lastID, ctx, client, dbname, collectionName), out, done, int64(oplogTime.T)); err != nil {
						log.With("db", dbname).Errorln(err)
						return
					}
					log.With("db", dbname).With("collection", collectionName).Infoln("iterating complete")
				}
				if r.tail {
					wg.Add(1)
					log.With("collection", collectionName).Infof("oplog start timestamp: %d", oplogTime)
					go func(wg *sync.WaitGroup, collectionName string, o primitive.Timestamp) {
						defer wg.Done()

						// c - collection
						errc := r.tailCollection(collectionName, ctx, client, dbname, o, out, done)
						for err := range errc {
							log.With("db", dbname).With("collection", collectionName).Errorln(err)
							return
						}
					}(&wg, collectionName, oplogTime)
				}
			}
			log.With("db", dbname).Infoln("Read completed")
			// this will block if we're tailing
			wg.Wait()
			return
		}()

		return out, nil
	}
}

func (r *Reader) listCollections(ctx context.Context, client *mongo.Client, dbName string, filterFn func(name string) bool) ([]string, error) {
	var colls []string

	// Retrieve all collection names first, without MongoDB-side filtering.
	collections, err := client.Database(dbName).ListCollectionNames(ctx, bson.D{{}})
	if err != nil {
		return nil, fmt.Errorf("failed to list collections in '%s' database: %v", dbName, err)
	}

	for _, c := range collections {
		if filterFn(c) && !strings.HasPrefix(c, "system.") {
			log.With("db", dbName).With("collection", c).Infoln("adding for iteration...")
			colls = append(colls, c)
		} else {
			log.With("db", dbName).With("collection", c).Infoln("skipping iteration...")
		}
	}

	return colls, nil
}

func (r *Reader) iterateCollection(in <-chan message.Msg, out chan<- client.MessageSet, done chan struct{}, origOplogTime int64) error {
	for {
		select {
		case msg, ok := <-in:
			if !ok {
				return nil
			}
			out <- client.MessageSet{
				Msg:       msg,
				Timestamp: origOplogTime,
			}
		case <-done:
			return errors.New("iteration cancelled")
		}
	}
}

func (r *Reader) iterate(lastID interface{}, ctx context.Context, client *mongo.Client, dbName string, collectionName string) <-chan message.Msg {
	msgChan := make(chan message.Msg)

	go func() {
		defer close(msgChan)

		canReissueQuery := r.requeryable(collectionName, ctx, client, dbName)

		// Create the query with catQuery, assuming it's adapted to return a cursor and an error
		cursor, err := r.catQuery(collectionName, lastID, ctx, client, dbName)
		if err != nil {
			log.With("database", dbName).With("collection", collectionName).Errorf("error setting up query, %s", err)
			return
		}
		defer cursor.Close(ctx)

		for {
			if !cursor.Next(ctx) {
				if err := cursor.Err(); err != nil {
					log.With("database", dbName).With("collection", collectionName).Errorf("error iterating results, %s", err)
					if canReissueQuery {
						time.Sleep(5 * time.Second)
						cursor, err = r.catQuery(collectionName, lastID, ctx, client, dbName) // Attempt to recreate the cursor
						if err != nil {
							log.With("database", dbName).With("collection", collectionName).Errorf("error reissuing query, %s", err)
							return
						}
						continue
					}
					return
				}
				break // Exit loop if no error but iteration is done
			}

			var result bson.M
			if err := cursor.Decode(&result); err != nil {
				log.With("database", dbName).With("collection", collectionName).Errorf("error decoding result, %s", err)
				return
			}

			if id, ok := result["_id"]; ok {
				lastID = id
			}
			// Assuming a conversion function exists that properly creates a message.Msg from result
			msgChan <- message.From(ops.Insert, collectionName, data.Data(result))
		}
	}()

	return msgChan
}

// catQuery assembles the query for fetching documents from a collection based on the lastID seen
// and any filters applied specifically to that collection. It has been adapted
// for the MongoDB Go Driver, thus it needs a tweak in the return type.
func (r *Reader) catQuery(c string, lastID interface{}, ctx context.Context, client *mongo.Client, dbName string) (*mongo.Cursor, error) {
	coll := client.Database(dbName).Collection(c)
	query := bson.M{}

	// Apply any collection-specific filters if present
	if f, ok := r.collectionFilters[c]; ok {
		for key, value := range f {
			query[key] = value
		}
	}

	// Condition to fetch documents newer than the lastID, if provided
	if lastID != nil {
		query["_id"] = bson.M{"$gt": lastID}
	}

	// Executing the find operation with the constructed query and sorting by "_id"
	findOptions := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})
	cursor, err := coll.Find(ctx, query, findOptions)
	if err != nil {
		return nil, err
	}

	return cursor, nil // Returning the cursor to iterate over the query results
}

func (r *Reader) requeryable(c string, ctx context.Context, client *mongo.Client, dbName string) bool {
	coll := client.Database(dbName).Collection(c)
	indexesCursor, err := coll.Indexes().List(ctx)
	if err != nil {
		log.With("database", dbName).With("collection", c).Errorf("unable to list indexes, %s", err)
		return false
	}
	defer indexesCursor.Close(ctx)

	var indexesList []bson.M
	if err = indexesCursor.All(ctx, &indexesList); err != nil {
		log.With("database", dbName).With("collection", c).Errorf("unable to decode indexes list, %s", err)
		return false
	}

	for _, index := range indexesList {
		if keys, ok := index["key"].(bson.M); ok {
			if _, ok := keys["_id"]; ok {
				var result bson.M
				err := coll.FindOne(ctx, bson.M{"_id": bson.M{"$exists": true}}, options.FindOne().SetProjection(bson.M{"_id": 1})).Decode(&result)
				if err != nil {
					log.With("database", dbName).With("collection", c).Errorf("unable to sample document, %s", err)
					return false
				}
				// Assuming a function 'sortable' exists to check the type of _id
				if _, ok := result["_id"]; ok {
					return true
				}
				return false
			}
		}
	}

	log.With("database", dbName).With("collection", c).Infoln("invalid _id, any issues copying will be aborted")
	return false
}

func sortable(id interface{}) bool {
	switch id.(type) {
	case primitive.ObjectID, string, float64, int64, time.Time:
		return true
	}
	return false
}

func (r *Reader) tailCollection(collectionName string, ctx context.Context, mongoClient *mongo.Client, dbName string, oplogTime primitive.Timestamp, out chan<- client.MessageSet, done chan struct{}) chan error {
	errc := make(chan error)

	go func() {
		defer close(errc)

		// Initialize the change stream for a given collection.
		pipeline := mongo.Pipeline{{{"$match", bson.D{{"operationType", bson.D{{"$in", []string{"insert", "update", "delete"}}}}}}}}
		opts := options.ChangeStream().SetFullDocument(options.UpdateLookup).SetStartAtOperationTime(&oplogTime)

		changeStream, err := mongoClient.Database(dbName).Collection(collectionName).Watch(ctx, pipeline, opts)
		if err != nil {
			log.With("database", dbName).With("collection", collectionName).Errorf("error opening change stream, %s", err)
			errc <- err
			return
		}
		defer changeStream.Close(ctx)

		for {
			select {
			case <-ctx.Done():
				log.With("database", dbName).With("collection", collectionName).Infoln("context done, stopping tailing...")
				return
			case <-done:
				log.With("database", dbName).With("collection", collectionName).Infoln("done signal received, stopping tailing...")
				return
			default:
				if changeStream.Next(ctx) {
					var event bson.M
					if err := changeStream.Decode(&event); err != nil {
						log.With("database", dbName).With("collection", collectionName).Errorf("error decoding change stream event, %s", err)
						continue
					}

					opType, opFound := event["operationType"].(string)
					if !opFound || (opType != "insert" && opType != "update" && opType != "delete") {
						// If operation type is not valid, skip this event.
						continue
					}

					// Process valid operation
					doc := event["fullDocument"].(bson.M) // For insert and update operations
					var op ops.Op
					switch opType {
					case "insert":
						op = ops.Insert
					case "update":
						op = ops.Update
					case "delete":
						op = ops.Delete
						// For delete operation, you might want to handle it differently,
						// as the full document might not be available.
					}

					msg := message.From(op, collectionName, data.Data(doc)).(*message.Base)
					// Construct and send the message set.
					out <- client.MessageSet{
						Msg:       msg,
						Timestamp: msg.Timestamp(), // Assuming a conversion method exists from the event to your timestamp format.
						Mode:      commitlog.Sync,  // Mode based on your logic.
					}
				}
			}
		}
	}()
	return errc
}

// getOriginalDoc retrieves the original document from the database.
// transporter has no knowledge of update operations, all updates work as wholesale document replaces
func (r *Reader) getOriginalDoc(doc bson.M, c string, s *mgo.Session) (result bson.M, err error) {
	id, exists := doc["_id"]
	if !exists {
		return result, fmt.Errorf("can't get _id from document")
	}

	query := bson.M{}
	if f, ok := r.collectionFilters[c]; ok {
		query = bson.M(f)
	}
	query["_id"] = id

	err = s.DB("").C(c).Find(query).One(&result)
	if err != nil {
		err = fmt.Errorf("%s.%s %v %v", s.DB("").Name, c, id, err)
	}
	return
}

// oplogDoc are representations of the mongodb oplog document
// detailed here, among other places.  http://www.kchodorow.com/blog/2010/10/12/replication-internals/
type oplogDoc struct {
	Ts primitive.Timestamp `bson:"ts"`
	H  int64               `bson:"h"`
	V  int                 `bson:"v"`
	Op string              `bson:"op"`
	Ns string              `bson:"ns"`
	O  bson.M              `bson:"o"`
	O2 bson.M              `bson:"o2"`
}

// validOp checks to see if we're an insert, delete, or update, otherwise the
// document is skilled.
func (o *oplogDoc) validOp(ns string) bool {
	return ns == o.Ns && (o.Op == "i" || o.Op == "d" || o.Op == "u")
}

func timeAsMongoTimestamp(t time.Time) primitive.Timestamp {
	return primitive.Timestamp{T: uint32(t.Unix()), I: uint32(1)} // Setting the increment (I) as 1 by default
}
