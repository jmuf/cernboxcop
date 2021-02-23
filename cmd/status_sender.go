package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/smtp"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var instance *bolt.DB
var once sync.Once

const failedProbesBucket = "FailedProbes"

type failedProbe struct {
	Nodes []string
	Time  time.Time
}

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
		if bucket := tx.Bucket([]byte(failedProbesBucket)); bucket != nil {
			pJSON := bucket.Get([]byte(p.Name))

			if pJSON == nil {
				isSent = p.IsSuccess
				return nil
			}
			probeFailed := new(failedProbe)
			err := json.Unmarshal(pJSON, probeFailed)
			if err != nil {
				return err
			}
			isSent = isListEquals(probeFailed.Nodes, p.GetListNodesFailed())

		}

		return nil
	})

	return isSent
}

func storeInfo(p Probe) {
	getInstance().Batch(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte(failedProbesBucket))

		bucket := tx.Bucket([]byte(failedProbesBucket))
		bucket.Delete([]byte(p.Name))

		if !p.IsSuccess {

			fProbe := failedProbe{Nodes: p.GetListNodesFailed(), Time: time.Now()}

			pJSON, err := json.Marshal(fProbe)
			if err != nil {
				return err
			}

			bucket.Put([]byte(p.Name), pJSON)
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

func sendStatusEmail(message string) {
	user, password := getEmailCredentials()
	from := getEmailSender()
	to := getEmails()

	smtpHost := "cernsmtp.cern.ch"
	smtpPort := "587"

	auth := smtp.PlainAuth("", user, password, smtpHost)

	headerBody := "Subject: EOS Probe: service degraded\r\n" +
		"\r\n" +
		message

	// Sending email.
	err := smtp.SendMail(smtpHost+":"+smtpPort, auth, from, to, []byte(headerBody))
	if err != nil {
		fmt.Println(err)
		return
	}
	if verbose {
		fmt.Println("Email Sent Successfully!")
		fmt.Printf("TO: %v\n", to)
		fmt.Printf("BODY:\n%v\n", message)
	}
}

func removeStatus(probe string) {
	getInstance().Update(func(tx *bolt.Tx) error {
		if bucket := tx.Bucket([]byte(failedProbesBucket)); bucket != nil {
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
