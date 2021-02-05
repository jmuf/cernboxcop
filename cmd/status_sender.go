package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var instance *bolt.DB
var once sync.Once

func getInstance() *bolt.DB {
	if instance == nil {

		once.Do(func() {
			instance, err := bolt.Open("db.db", 0600, nil)
			if err == nil {
				// TODO
			}
		})

	}
	return instance
}

func isAlreadySent(service, info string) bool {
	var isSent bool

	getInstance().Batch(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(service))
		isSent = bucket != nil && bucket.Get([]byte(info)) != nil
		return nil
	})

	return isSent
}

func storeInfo(service, info string) {
	getInstance().Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(service))
		if err == nil {
			return err
		}
		now := time.Now().Unix()
		err = bucket.Put([]byte(info), []byte(string(now)))
		if err == nil {
			return err
		}
		return nil
	})
}

// SendStatus :::TODO:::
func SendStatus(status, service, err string) {
	// check if an email is already sent from the db
	if !isAlreadySent(service, err) {
		storeInfo(service, err)
		send(status, fmt.Sprintf("%s %s", service, err))
	}
}

// RemoveErrors :::TODO:::
func RemoveErrors(service string) {
	getInstance().Update(func(tx *bolt.Tx) error {
		tx.DeleteBucket([]byte(service))
		return nil
	})
}

func send(status, info string) {
	msg := map[string]interface{}{
		"producer":         "cernbox",
		"type":             "availability",
		"serviceid":        "cernbox",
		"service_status":   status,
		"timestamp":        time.Now().Unix(),
		"availabilitydesc": "Indicates availability of the CERNBox service and underlying EOS instances",
		"availabilityinfo": info,
		"contact":          "cernbox-admins@cern.ch",
		"webpage":          "http://cern.ch/cernbox",
	}

	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(msg); err != nil {
		er(err)
	}
	req, err := http.NewRequest("POST", "http://monit-metrics.cern.ch:10012", buf)
	if err != nil {
		er(err)
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		er(err)
	}
	if res.StatusCode != http.StatusOK {
		fmt.Println("Uploading metrics to monit-metrics.cern.ch:10012 failed")
	}

	fmt.Printf("Availability status: %s\nInfo: %s\n", status, info)
}
