package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(metricsCmd)
	metricsCmd.AddCommand(availabilityCmd)
}

var metricsCmd = &cobra.Command{
	Use:   "metrics",
	Short: "CERNBox service metrics",
}

var availabilityCmd = &cobra.Command{
	Use:   "availability",
	Short: "Checks the CERNBox HTTP service and EOS instances for availability",
	Run: func(cmd *cobra.Command, args []string) {

		text := "dummy text with time " + time.Now().String()

		user, password := getProbeUser()
		if user == "" || password == "" {
			er("please set probe_user and probe_password in the config")
		}

		serverURL := fmt.Sprintf("https://cernbox.cern.ch/cernbox/desktop/remote.php/webdav/eos/user/%s/%s/sls", user[:1], user)
		httpClient := &http.Client{}

		// Create the remote folder
		mkdirReq, err := http.NewRequest("MKCOL", serverURL, nil)
		if err != nil {
			er(err)
		}
		mkdirReq.SetBasicAuth(user, password)
		mkdirRes, err := httpClient.Do(mkdirReq)
		if err != nil {
			er(err)
		}
		mkdirRes.Body.Close()
		if mkdirRes.StatusCode != http.StatusOK && mkdirRes.StatusCode != http.StatusCreated {
			sendStatus("degraded", "WebDAV MKCOL calls are failing")
			return
		}

		// Upload file to OC server
		uploadReq, err := http.NewRequest("PUT", serverURL+"/dummy.txt", strings.NewReader(text))
		if err != nil {
			er(err)
		}
		uploadReq.SetBasicAuth(user, password)
		uploadRes, err := httpClient.Do(uploadReq)
		if err != nil {
			er(err)
		}
		uploadRes.Body.Close()
		if uploadRes.StatusCode != http.StatusOK && uploadRes.StatusCode != http.StatusCreated {
			sendStatus("degraded", "WebDAV uploads are failing")
			return
		}

		// Download the file
		downloadReq, err := http.NewRequest("GET", serverURL+"/dummy.txt", nil)
		if err != nil {
			er(err)
		}
		downloadReq.SetBasicAuth(user, password)
		downloadRes, err := httpClient.Do(downloadReq)
		if err != nil {
			er(err)
		}
		defer downloadRes.Body.Close()
		if downloadRes.StatusCode != http.StatusOK {
			sendStatus("degraded", "WebDAV downloads are failing")
			return
		}
		body, err := ioutil.ReadAll(downloadRes.Body)
		if err != nil {
			er(err)
		}
		if string(body) != text {
			sendStatus("degraded", "WebDAV downloads are failing")
			return
		}

		info := "WebDAV and xrdcopy transfers fully operational"
		status := "available"

		mgms := getProbeEOSInstances()
		var failedMGMs []string
		errors := make([]error, len(mgms))
		var wg sync.WaitGroup

		for i, m := range mgms {
			wg.Add(1)
			go xrdcpTest(m, user, &errors[i], &wg)
		}
		wg.Wait()

		for i := range mgms {
			if errors[i] != nil {
				failedMGMs = append(failedMGMs, mgms[i])
			}
		}

		if len(failedMGMs) > 0 {
			status = "degraded"
			info = "WebDAV transfers fully operational; xrdcopy tests failing on MGMs: " + strings.Join(failedMGMs, ", ")
		}

		sendStatus(status, info)

	},
}

func xrdcpTest(mgm, user string, e *error, wg *sync.WaitGroup) {
	defer wg.Done()
	eosClient := getEOS(fmt.Sprintf("root://%s.cern.ch", mgm))

	text := "dummy text with time " + time.Now().String()
	reader := strings.NewReader(text)
	ctx := getCtx()

	if err := eosClient.Write(ctx, user, fmt.Sprintf("/eos/%s/opstest/sls/dummy.txt", strings.TrimPrefix(mgm, "eos")), ioutil.NopCloser(reader)); err != nil {
		*e = err
		return
	}

	if body, err := eosClient.Read(ctx, user, fmt.Sprintf("/eos/%s/opstest/sls/dummy.txt", strings.TrimPrefix(mgm, "eos"))); err != nil {
		*e = err
		return
	} else {
		data, err := ioutil.ReadAll(body)
		if err != nil {
			*e = err
			return
		}
		if string(data) != text {
			*e = fmt.Errorf("Original text and that returned by MGM don't match")
			return
		}
	}
}

func sendStatus(status, info string) {
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
