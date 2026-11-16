package mongo

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

type Command string

const (
	Unknown           Command = "unknown"
	AbortTransaction  Command = "abortTransaction"
	Aggregate         Command = "aggregate"
	CommitTransaction Command = "commitTransaction"
	Count             Command = "count"
	CreateIndexes     Command = "createIndexes"
	Delete            Command = "delete"
	Distinct          Command = "distinct"
	Drop              Command = "drop"
	DropDatabase      Command = "dropDatabase"
	DropIndexes       Command = "dropIndexes"
	EndSessions       Command = "endSessions"
	Find              Command = "find"
	FindAndModify     Command = "findAndModify"
	GetMore           Command = "getMore"
	Hello             Command = "hello"
	Insert            Command = "insert"
	IsMaster          Command = "isMaster"
	Ismaster          Command = "ismaster"
	ListCollections   Command = "listCollections"
	ListIndexes       Command = "listIndexes"
	ListDatabases     Command = "listDatabases"
	MapReduce         Command = "mapReduce"
	Update            Command = "update"
)

var collectionCommands = []Command{Aggregate, Count, CreateIndexes, Delete, Distinct, Drop, DropIndexes, Find, FindAndModify, Insert, ListIndexes, MapReduce, Update}
var int32Commands = []Command{AbortTransaction, Aggregate, CommitTransaction, DropDatabase, Hello, IsMaster, Ismaster, ListCollections, ListDatabases}
var int64Commands = []Command{GetMore}
var arrayCommands = []Command{EndSessions}

func IsWrite(command Command) bool {
	switch command {
	case CommitTransaction, CreateIndexes, Delete, Drop, DropIndexes, DropDatabase, FindAndModify, Insert, Update:
		return true
	}
	return false
}

func CommandAndCollection(msg bsoncore.Document) (Command, string) {
	for _, s := range collectionCommands {
		if coll, ok := msg.Lookup(string(s)).StringValueOK(); ok {
			return s, coll
		}
	}
	for _, s := range int32Commands {
		value := msg.Lookup(string(s))
		if value.Data != nil {
			return s, ""
		}
	}
	for _, s := range int64Commands {
		value := msg.Lookup(string(s))
		if value.Data != nil {
			if coll, ok := msg.Lookup("collection").StringValueOK(); ok {
				return s, coll
			}
			return s, ""
		}
	}
	for _, s := range arrayCommands {
		value := msg.Lookup(string(s))
		if value.Data != nil {
			return s, ""
		}
	}
	return Unknown, ""
}

// HandshakeCommandOf returns Hello, IsMaster or Unknown depending on whether doc is a truthy handshake command.
func HandshakeCommandOf(doc bsoncore.Document) Command {
	if IsIsMasterValueTruthy(doc.Lookup(string(Hello))) {
		return Hello
	}
	if IsIsMasterValueTruthy(doc.Lookup(string(IsMaster))) || IsIsMasterValueTruthy(doc.Lookup(string(Ismaster))) {
		return IsMaster
	}
	return Unknown
}

func IsIsMasterDoc(doc bsoncore.Document) bool {
	return HandshakeCommandOf(doc) != Unknown
}

// IsIsMasterValueTruthy reports whether val is an int32/int64/double > 0 or boolean true.
func IsIsMasterValueTruthy(val bsoncore.Value) bool {
	switch val.Type {
	case bson.TypeInt32:
		return val.Int32() > 0
	case bson.TypeInt64:
		return val.Int64() > 0
	case bson.TypeDouble:
		return val.Double() > 0
	case bson.TypeBoolean:
		return val.Boolean()
	}
	return false
}
