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

	once.Do(func() {
		instance, _ = bolt.Open(getStatusSenderDB(), 0600, nil)
	})

	return instance
}

func isInList(list []string, v string) bool {
	for _, e := range list {
		if e == v {
			return true
		}
	}
	return false
}

func isListEquals(l1, l2 []string) bool {
	if len(l1) != len(l2) {
		return false
	}
	for _, e1 := range l1 {
		if !isInList(l2, e1) {
			return false
		}
	}
	return true
}

func isAlreadySent(p *Probe) bool {
	isSent := false

	getInstance().Batch(func(tx *bolt.Tx) error {
		if bucket := tx.Bucket([]byte("Probes")); bucket != nil {
			info := bucket.Get([]byte(p.Name))

			if info == nil {
				isSent = p.IsSuccess
				return nil
			}

			// reading and decoding
			reader := bytes.NewReader(info)

			var probePrevStatus map[string]interface{}
			json.NewDecoder(reader).Decode(&probePrevStatus)

			var nodes []string
			for _, v := range probePrevStatus["nodes"].([]interface{}) {
				nodes = append(nodes, v.(string))
			}
			isSent = isListEquals(nodes, p.GetListNodesFailed())

		}

		return nil
	})

	return isSent
}

func storeInfo(p Probe) {
	getInstance().Batch(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("Probes"))

		bucket := tx.Bucket([]byte("Probes"))
		bucket.Delete([]byte(p.Name))

		if !p.IsSuccess {

			msg := map[string]interface{}{"nodes": p.GetListNodesFailed(), "time": time.Now().Unix()}

			var buf bytes.Buffer
			json.NewEncoder(&buf).Encode(msg)
			info := buf.Bytes()

			bucket.Put([]byte(p.Name), info)
		}

		return nil
	})
}

func generateStatusMessage(listProbes *[]*Probe) string {
	var info string = ""
	for _, probe := range *listProbes {
		info += fmt.Sprintf("%s: service ", probe.Name)

		if probe.IsSuccess {
			info += "available\n"
		} else {
			info += "degraded. Failed on: "
			failedNodes := probe.GetListNodesFailed()
			for i, n := range failedNodes {
				info += n
				if i == len(failedNodes)-1 {
					info += ".\n"
				} else {
					info += ", "
				}
			}
		}

	}
	return info
}

func getStatus(listProbes *[]*Probe) string {
	for _, p := range *listProbes {
		if !p.IsSuccess {
			return "degraded"
		}
	}
	return "available"
}

// SendStatus :::TODO:::
func SendStatus(listProbes *[]*Probe) {

	status := getStatus(listProbes)
	info := generateStatusMessage(listProbes)

	// always send metric status to CERN monitoring service
	sendMetricStatus(status, info)
	if verbose {
		fmt.Printf("Sending Metric Status\n    status: %s, info: %s\n", status, info)
	}

	// send email only if not already sent in a previous run
	sendEmail := false

	for _, probe := range *listProbes {

		if probe.IsSuccess {
			removeStatus(probe.Name)
			continue
		}
		if isAlreadySent(probe) {
			if verbose {
				fmt.Printf("%s still has same issues as previous run\n", probe.Name)
			}
			continue
		}
		sendEmail = true
	}

	if sendEmail {
		for _, p := range *listProbes {
			storeInfo(*p)
		}
		sendStatusEmail(info)

	} else {
		if verbose {
			fmt.Println("\nStatus email has been already sent")
		}
	}
}

func sendStatusEmail(email string) {
	fmt.Println("Sending Emails: TODO")
	if verbose {
		fmt.Println("\nStatus email sent")
	}
}

func removeStatus(probe string) {
	getInstance().Update(func(tx *bolt.Tx) error {
		if bucket := tx.Bucket([]byte("Probes")); bucket != nil {
			bucket.Delete([]byte(probe))
		}
		// tx.DeleteBucket([]byte(service))
		return nil
	})
}

// Sends the metric status to the CERN monitoring service
func sendMetricStatus(status, info string) {
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

	// fmt.Printf("Availability status: %s\nInfo: %s\n", status, info)
}
