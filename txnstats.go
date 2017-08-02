package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strings"

	"github.com/juju/utils/parallel"
	errgo "gopkg.in/errgo.v1"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Stats struct {
	Collections map[string]CollectionStats
	InProgress  InProgressStats
	Log         LogStats
}

type CollectionStats struct {
	DocCount    int `json:",omitempty"`
	MaxQueued   int `json:",omitempty"`
	MinQueued   int `json:",omitempty"`
	TotalQueued int `json:",omitempty"`
	// TODO count bad tokens too?
}

type LogStats struct {
	DocCount int `json:",omitempty"`
}

type InProgressStats struct {
	States    map[state]int
	MaxOps    int `json:",omitempty"`
	TotalOps  int `json:",omitempty"`
	TotalTxns int `json:",omitempty"`
}

func main() {
	args := commandLine()
	session, err := dial(args)
	if err != nil {
		log.Fatal("cannot dial mongodb: %v", err)
	}
	db := session.DB("juju")
	collNames, err := db.CollectionNames()
	if err != nil {
		log.Fatal("cannot dial mongodb: %v", err)
	}
	sort.Strings(collNames)
	collStats := make([]CollectionStats, len(collNames))
	run := parallel.NewRun(10)
	for i, collName := range collNames {
		if !wantCollectionStats(collName) {
			continue
		}
		i, collName := i, collName
		run.Do(func() error {
			c := db.C(collName)

			stats, err := getCollectionStats(c)
			if err != nil {
				return errgo.Notef(err, "cannot gather stats on %s: %v", c.Name, err)
			}
			collStats[i] = stats
			return nil
		})
	}
	stats := &Stats{
		Collections: make(map[string]CollectionStats),
	}
	run.Do(func() error {
		logStats, err := getLogStats(db.C("txns.log"))
		if err != nil {
			return errgo.Mask(err)
		}
		stats.Log = logStats
		return nil
	})
	run.Do(func() error {
		inProgressStats, err := getInProgressStats(db.C("txns"))
		if err != nil {
			return errgo.Mask(err)
		}
		stats.InProgress = inProgressStats
		return nil
	})

	if err := run.Wait(); err != nil {
		log.Fatal(err)
	}
	for i, collName := range collNames {
		if !wantCollectionStats(collName) {
			continue
		}
		if collStats[i].TotalQueued > 0 {
			stats.Collections[collName] = collStats[i]
		}
	}
	data, _ := json.MarshalIndent(stats, "", "\t")
	os.Stdout.Write(data)
}

func wantCollectionStats(collName string) bool {
	return collName != "txns" && !strings.HasPrefix(collName, "system.")
}

func getLogStats(c *mgo.Collection) (LogStats, error) {
	n, err := c.Count()
	if err != nil {
		return LogStats{}, errgo.Notef(err, "cannot count items in collection %q", c.Name)
	}
	return LogStats{
		DocCount: n,
	}, nil
}

func getInProgressStats(c *mgo.Collection) (InProgressStats, error) {
	iter := c.Find(nil).Select(bson.M{"o": 1, "s": 1}).Iter()
	var doc txnDoc
	var stats InProgressStats
	stats.States = make(map[state]int)
	for iter.Next(&doc) {
		stats.TotalTxns++
		stats.TotalOps += len(doc.Ops)
		if len(doc.Ops) > stats.MaxOps {
			stats.MaxOps = len(doc.Ops)
		}
		stats.States[doc.State]++
	}
	if err := iter.Err(); err != nil {
		return stats, errgo.Notef(err, "iteraction over collection %q failed", c.Name)
	}
	return stats, nil
}

func getCollectionStats(c *mgo.Collection) (CollectionStats, error) {
	// TODO if we could rely on mongo 2.6 or later, we could use
	// an aggregation pipeline here rather than pulling all the txn-queue
	// elements individually.
	iter := c.Find(nil).Batch(1000).Select(bson.M{"txn-queue": 1}).Iter()
	var doc docDoc
	var stats CollectionStats
	for first := true; iter.Next(&doc); {
		stats.DocCount++
		stats.TotalQueued += len(doc.Queue)
		if first {
			stats.MaxQueued = len(doc.Queue)
			stats.MinQueued = len(doc.Queue)
			first = false
		} else {
			if len(doc.Queue) > stats.MaxQueued {
				stats.MaxQueued = len(doc.Queue)
			}
			if len(doc.Queue) < stats.MinQueued {
				stats.MinQueued = len(doc.Queue)
			}
		}
	}
	if err := iter.Err(); err != nil {
		return stats, errgo.Notef(err, "iteraction over collection %q failed", c.Name)
	}
	return stats, nil
}

type commandLineArgs struct {
	hostname string
	port     string
	ssl      bool
	username string
	password string
}

func commandLine() commandLineArgs {
	flags := flag.NewFlagSet("mgopurge", flag.ExitOnError)
	var a commandLineArgs
	flags.StringVar(&a.hostname, "hostname", "localhost",
		"hostname of the Juju MongoDB server")
	flags.StringVar(&a.port, "port", "37017",
		"port of the Juju MongoDB server")
	flags.BoolVar(&a.ssl, "ssl", true,
		"use SSL to connect to MonogDB ")
	flags.StringVar(&a.username, "username", "admin",
		"user for connecting to MonogDB (use \"\" to for no authentication)")
	flags.StringVar(&a.password, "password", "",
		"password for connecting to MonogDB")

	flags.Parse(os.Args[1:])

	if a.password == "" && a.username != "" {
		fmt.Fprintf(os.Stderr, "error: -password must be used if username is provided\n")
		os.Exit(2)
	}
	return a
}

func dial(args commandLineArgs) (*mgo.Session, error) {
	info := &mgo.DialInfo{
		Addrs: []string{net.JoinHostPort(args.hostname, args.port)},
	}
	if args.username != "" {
		info.Database = "admin"
		info.Username = args.username
		info.Password = args.password
	}
	if args.ssl {
		info.DialServer = dialSSL
	}
	session, err := mgo.DialWithInfo(info)
	if err != nil {
		return nil, err
	}
	return session, nil
}

func dialSSL(addr *mgo.ServerAddr) (net.Conn, error) {
	c, err := net.Dial("tcp", addr.String())
	if err != nil {
		return nil, err
	}
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	cc := tls.Client(c, tlsConfig)
	if err := cc.Handshake(); err != nil {
		return nil, err
	}
	return cc, nil
}
