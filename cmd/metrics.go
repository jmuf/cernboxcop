package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
)

func init() {
	nsStatCmd.Flags().StringP("carbon-server", "c", "filer-carbon.cern.ch:2003", "graphite server")
	nsStatCmd.Flags().StringP("prefix", "p", "test.cernbox.newmetrics3.eos.ns-stats", "namespace for metrics")

	rootCmd.AddCommand(metricsCmd)
	metricsCmd.AddCommand(availabilityCmd)
	metricsCmd.AddCommand(nsStatCmd)
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

var nsStatCmd = &cobra.Command{
	Use:   "eos-ns-stat",
	Short: "Retrieves operation calls per user",
	Run: func(cmd *cobra.Command, args []string) {

		metrics := map[string]float64{}
		mgms := []string{"eoshome-i00", "eoshome-i01", "eoshome-i02", "eoshome-i03", "eoshome-i04", "eosproject-i00", "eosproject-i01", "eosproject-i02"}
		for _, i := range mgms {
			a := `eos -r 0 0 ns stat -m -a | grep cmd | grep -v gid | sed 's/=/ /g' | grep -v 'root cmd' | awk '{print $2,$4,$12}'`
			c := exec.Command("/usr/bin/bash", "-c", a)
			m := fmt.Sprintf("root://%s.cern.ch", i)
			c.Env = []string{
				"EOS_MGM_URL=" + m,
			}
			ctx, _ := context.WithTimeout(context.Background(), time.Second*30)
			o, e, err := execute(ctx, c)
			if err != nil {
				fmt.Fprintln(os.Stdout, "stdout", o)
				fmt.Fprintln(os.Stderr, "stderr", e)
				er(err)
			}

			lines := strings.Split(o, "\n")
			for _, l := range lines {
				l = strings.TrimSpace(l)
				if l == "" {
					continue
				}
				tokens := strings.Split(l, " ")
				username := tokens[0]
				op := tokens[1]
				op = strings.ReplaceAll(op, "::", "-")
				hz5m := tokens[2]
				hz5mFloat, _ := strconv.ParseFloat(hz5m, 64)
				hz5mFloat = hz5mFloat * 60 * 5 // get total ops per 5 min slot
				key := fmt.Sprintf("%s.%s.%s", i, username, op)
				metrics[key] = hz5mFloat
			}

		}

		// create tcp connection
		server, _ := cmd.Flags().GetString("carbon-server")
		prefix, _ := cmd.Flags().GetString("prefix")
		conn, err := net.Dial("tcp", server)
		if err != nil {
			er(err)
		}

		now := time.Now().Unix()
		format := "%s.%s %f %d\n"
		for k, v := range metrics {
			payload := fmt.Sprintf(format, prefix, k, v, now)
			fmt.Print(payload)
			if _, err := conn.Write([]byte(payload)); err != nil {
				er(err)
			}
		}

	},
}

func execute(ctx context.Context, cmd *exec.Cmd) (string, string, error) {
	outBuf := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	cmd.Stdout = outBuf
	cmd.Stderr = errBuf

	err := cmd.Run()
	if err != nil {
		return outBuf.String(), errBuf.String(), err
	}

	return outBuf.String(), errBuf.String(), err
}
