package main

import (
	"context"
	"fmt"
	"os"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func mongoV1Workflow(ctx context.Context) (any, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("MONGO_URL")))
	if err != nil {
		return nil, err
	}
	defer client.Disconnect(context.Background())
	coll := client.Database("repro_test").Collection("orders_v1")
	id := bson.M{"_id": "legacy-order"}
	if _, err = coll.InsertOne(ctx, bson.M{"_id": "legacy-order", "amount": 1099}); err != nil {
		return nil, err
	}
	if _, err = coll.UpdateOne(ctx, id, bson.M{"$set": bson.M{"amount": 1599}}); err != nil {
		return nil, err
	}
	var found bson.M
	if err = coll.FindOne(ctx, id).Decode(&found); err != nil {
		return nil, err
	}
	cursor, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	var all []bson.M
	if err = cursor.All(ctx, &all); err != nil {
		return nil, err
	}
	if len(all) != 1 || found["amount"] != int32(1599) {
		return nil, fmt.Errorf("v1 driver results changed")
	}
	if err = coll.FindOne(ctx, bson.M{"_id": "missing"}).Decode(&bson.M{}); err != mongo.ErrNoDocuments {
		return nil, fmt.Errorf("missing document error changed")
	}
	return map[string]any{"found": found, "all": all}, nil
}
