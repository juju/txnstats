package main

import (
	"fmt"

	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2/txn"
)

type txnDoc struct {
	Id    bson.ObjectId `bson:"_id"`
	State state         `bson:"s"`
	Ops   []txn.Op      `bson:"o"`
	Nonce string        `bson:"n,omitempty"`
}

// txnInfo is the schema used by the txns.stash
// collection. Note that the txn-queue member
// is compatible with docInfo.
type stashDoc struct {
	docDoc `bson:",inline"`
	Queue  []string      `bson:"txn-queue"`
	Revno  int64         `bson:"txn-revno,omitempty"`
	Insert bson.ObjectId `bson:"txn-insert,omitempty"`
	Remove bson.ObjectId `bson:"txn-remove,omitempty"`
}

type docDoc struct {
	Queue []string `bson:"txn-queue"`
}

type token string

type state int

const (
	tinvalid   state = 0
	tpreparing state = 1 // One or more documents not prepared
	tprepared  state = 2 // Prepared but not yet ready to run
	taborting  state = 3 // Assertions failed, cleaning up
	tapplying  state = 4 // Changes are in progress
	taborted   state = 5 // Pre-conditions failed, nothing done
	tapplied   state = 6 // All changes applied
	numStates  state = 7
)

func (s state) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

func (s state) String() string {
	switch s {
	case tinvalid:
		return "invalid"
	case tpreparing:
		return "preparing"
	case tprepared:
		return "prepared"
	case taborting:
		return "aborting"
	case tapplying:
		return "applying"
	case taborted:
		return "aborted"
	case tapplied:
		return "applied"
	}
	return fmt.Sprintf("unknown state: %d", s)
}
