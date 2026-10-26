package store

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"minidanmaku/internal/model"
)

const danmakuCollection = "danmaku"

// MongoStore is a Store backed by MongoDB.
type MongoStore struct {
	client      *mongo.Client
	col         *mongo.Collection
	indexCtx    context.Context
	indexCancel context.CancelFunc
}

// NewMongoStore connects and returns a store. It does not fail hard when Mongo
// is unreachable: the driver reconnects in the background and index creation
// retries until it succeeds, so the server can start (and keep broadcasting)
// even while Mongo is temporarily down.
func NewMongoStore(parent context.Context, uri, db string, logger *slog.Logger) (*MongoStore, error) {
	client, err := mongo.Connect(parent, options.Client().ApplyURI(uri).SetServerSelectionTimeout(3*time.Second))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	m := &MongoStore{
		client:      client,
		col:         client.Database(db).Collection(danmakuCollection),
		indexCtx:    ctx,
		indexCancel: cancel,
	}
	go m.ensureIndexes(logger)
	return m, nil
}

// ensureIndexes creates the two indexes that back the real query patterns,
// retrying with backoff until Mongo is reachable or the store is closed.
func (m *MongoStore) ensureIndexes(logger *slog.Logger) {
	indexes := []mongo.IndexModel{
		{
			// History: "by room, newest-first", with _id as a stable tiebreak
			// and keyset-pagination cursor.
			Keys:    bson.D{{Key: "room", Value: 1}, {Key: "ts", Value: -1}, {Key: "_id", Value: -1}},
			Options: options.Index().SetName("room_ts_id"),
		},
		{
			// Per-room monotonic sequence; supports "seq > lastSeen" catch-up.
			Keys:    bson.D{{Key: "room", Value: 1}, {Key: "seq", Value: 1}},
			Options: options.Index().SetUnique(true).SetName("room_seq_unique"),
		},
	}

	backoff := time.Second
	for {
		ctx, cancel := context.WithTimeout(m.indexCtx, 10*time.Second)
		_, err := m.col.Indexes().CreateMany(ctx, indexes)
		cancel()
		if err == nil {
			return
		}
		if m.indexCtx.Err() != nil {
			return // shutting down
		}
		logger.Warn("index creation failed, retrying", "err", err, "backoff", backoff)
		select {
		case <-time.After(backoff):
		case <-m.indexCtx.Done():
			return
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// Insert writes one danmaku. Its _id is the server-assigned message id, so a
// duplicate re-insert (same id) is treated as success — making the async
// retry path idempotent.
func (m *MongoStore) Insert(ctx context.Context, d *model.Danmaku) error {
	_, err := m.col.InsertOne(ctx, d)
	if mongo.IsDuplicateKeyError(err) {
		return nil
	}
	return err
}

// Query returns the newest danmaku in `room`, optionally older than `from`.
func (m *MongoStore) Query(ctx context.Context, room string, from int64, limit int) ([]*model.Danmaku, error) {
	filter := bson.M{"room": room}
	if from > 0 {
		filter["ts"] = bson.M{"$lt": from}
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "ts", Value: -1}, {Key: "_id", Value: -1}}).
		SetLimit(int64(limit))

	cur, err := m.col.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	out := make([]*model.Danmaku, 0, limit)
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Close cancels index creation and disconnects.
func (m *MongoStore) Close(ctx context.Context) error {
	m.indexCancel()
	return m.client.Disconnect(ctx)
}
