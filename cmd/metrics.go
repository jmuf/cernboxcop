package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"

	"github.com/spf13/cobra"
)

func init() {
	//	nsStatCmd.Flags().StringP("carbon-server", "c", "filer-carbon.cern.ch:2003", "graphite server")
	//	nsStatCmd.Flags().StringP("prefix", "p", "test.cernbox.newmetrics3.eos.ns-stats", "namespace for metrics")

	rootCmd.AddCommand(metricsCmd)
	metricsCmd.AddCommand(availabilityCmd)
	metricsCmd.AddCommand(nsStatCmd)
	metricsCmd.AddCommand(ioStatCmd)
	metricsCmd.AddCommand(quotaCmd)
}

var metricsCmd = &cobra.Command{
	Use:   "metrics",
	Short: "CERNBox service metrics",
}

func webDavTest(user string, password string) {
	text := "dummy text with time " + time.Now().String()

	serverURL := fmt.Sprintf("https://cernbox.cern.ch/cernbox/desktop/remote.php/webdav/eos/user/%s/%s/sls", user[:1], user)
	httpClient := &http.Client{}

	// Create the remote folder
	mkdirReq, err := http.NewRequest("MKCOL", serverURL, nil)
	check(err)

	mkdirReq.SetBasicAuth(user, password)
	mkdirRes, err := httpClient.Do(mkdirReq)
	check(err)

	mkdirRes.Body.Close()
	if mkdirRes.StatusCode != http.StatusOK && mkdirRes.StatusCode != http.StatusCreated {
		sendStatus("degraded", "WebDAV MKCOL calls are failing")
		return
	}

	// Upload file to OC server
	uploadReq, err := http.NewRequest("PUT", serverURL+"/dummy.txt", strings.NewReader(text))
	check(err)

	uploadReq.SetBasicAuth(user, password)
	uploadRes, err := httpClient.Do(uploadReq)
	check(err)

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
	check(err)

	defer downloadRes.Body.Close()
	if downloadRes.StatusCode != http.StatusOK {
		sendStatus("degraded", "WebDAV downloads are failing")
		return
	}
	body, err := ioutil.ReadAll(downloadRes.Body)
	check(err)

	if string(body) != text {
		sendStatus("degraded", "WebDAV downloads are failing")
		return
	}
}

var availabilityCmd = &cobra.Command{
	Use:   "availability",
	Short: "Checks the CERNBox HTTP service and EOS instances for availability",
	Run: func(cmd *cobra.Command, args []string) {

		user, password := getProbeUser()
		if user == "" || password == "" {
			er("please set probe_user and probe_password in the config")
		}

		webDavTest(user, password)

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

var ioStatCmd = &cobra.Command{
	Use:   "eos-io-stat",
	Short: "Retrieves IO operation calls per user",
	Run: func(cmd *cobra.Command, args []string) {
		influxUsername := viper.GetString("influx_username")
		influxPassword := viper.GetString("influx_password")
		influxHostname := viper.GetString("influx_hostname")
		influxPort := viper.GetInt("influx_port")
		type fourTuple struct {
			instance string
			username string
			op       string
			value    float64
		}
		tuples := []*fourTuple{}
		mgms := []string{"eoshome-i00", "eoshome-i01", "eoshome-i02", "eoshome-i03", "eoshome-i04", "eosproject-i00", "eosproject-i01", "eosproject-i02"}
		for _, i := range mgms {
			a := `eos -r 0 0 io stat -a -m | grep uid | less | sed 's/=/ /g' | awk '{print $2, $4, $12}'`
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
				if len(tokens) < 3 {
					continue
				}
				username := tokens[0]
				op := tokens[1]
				op = strings.ReplaceAll(op, "::", "-")
				volume60m := tokens[2]
				volume60mFloat, _ := strconv.ParseFloat(volume60m, 64)

				tuples = append(tuples, &fourTuple{
					instance: i,
					username: username,
					op:       op,
					value:    volume60mFloat,
				})
			}

		}

		lines := []string{}
		for _, t := range tuples {
			line := fmt.Sprintf("ioops,instance=%s,op=%s,username=%s value=%f %d", t.instance, t.op, t.username, t.value, time.Now().UnixNano())
			lines = append(lines, line)
		}

		client := &http.Client{}
		limit := len(lines) / 2000 // 2K chunks are under 5K batch size recommendation for influx
		for j := 0; j < len(lines); j += limit {
			batch := lines[j:min(j+limit, len(lines))]
			body := strings.Join(batch, "\n")
			url := fmt.Sprintf("https://%s:%d/write?db=eos", influxHostname, influxPort)
			req, err := http.NewRequest("POST", url, strings.NewReader(body))
			if err != nil {
				er(err)
			}
			req.SetBasicAuth(influxUsername, influxPassword)
			res, err := client.Do(req)
			if err != nil {
				er(err)
			}

			if res.StatusCode != http.StatusNoContent {
				body, _ := ioutil.ReadAll(res.Body)
				errString := fmt.Sprintf("failed to write to influxdb: %v %v", res.StatusCode, string(body))
				err := errors.New(errString)
				er(err)
			}
		}

	},
}

var quotaCmd = &cobra.Command{
	Use:   "eos-quota",
	Short: "Retrieves quotas",
	Run: func(cmd *cobra.Command, args []string) {
		influxUsername := viper.GetString("influx_username")
		influxPassword := viper.GetString("influx_password")
		influxHostname := viper.GetString("influx_hostname")
		influxPort := viper.GetInt("influx_port")
		type tuple struct {
			instance string
			username string
			op       string
			value    float64
		}
		tuples := []*tuple{}
		mgms := []string{"eoshome-i00", "eoshome-i01", "eoshome-i02", "eoshome-i03", "eoshome-i04", "eosproject-i00", "eosproject-i01", "eosproject-i02"}
		for _, i := range mgms {
			a := `eos -r 0 0 quota ls -m | sed 's/=/ /g' |  awk '{print $4,$6,$10,$12,$16,$18}'`
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
				// line is:
				// acontesc /eos/home-i01/opstest/acontesc/ 677935596413 463 750000000000000 1000000
				tokens := strings.Split(l, " ")
				if len(tokens) < 6 {
					continue
				}
				username := tokens[0]
				space := tokens[1]
				usedBytesString := tokens[2]
				usedFilesString := tokens[3]
				maxBytesString := tokens[4]
				maxFilesString := tokens[5]

				if !strings.HasPrefix(space, "/eos/user") && !strings.HasPrefix(space, "/eos/project") {
					continue
				}

				usedBytesFloat, _ := strconv.ParseFloat(usedBytesString, 64)
				usedFilesFloat, _ := strconv.ParseFloat(usedFilesString, 64)
				maxBytesFloat, _ := strconv.ParseFloat(maxBytesString, 64)
				maxFilesFloat, _ := strconv.ParseFloat(maxFilesString, 64)

				tuples = append(tuples, &tuple{
					instance: i,
					username: username,
					op:       "usedbytes",
					value:    usedBytesFloat,
				})
				tuples = append(tuples, &tuple{
					instance: i,
					username: username,
					op:       "maxbytes",
					value:    maxBytesFloat,
				})
				tuples = append(tuples, &tuple{
					instance: i,
					username: username,
					op:       "usedfiles",
					value:    usedFilesFloat,
				})
				tuples = append(tuples, &tuple{
					instance: i,
					username: username,
					op:       "maxfiles",
					value:    maxFilesFloat,
				})
			}

		}

		lines := []string{}
		for _, t := range tuples {
			line := fmt.Sprintf("quotas,instance=%s,op=%s,username=%s value=%f %d", t.instance, t.op, t.username, t.value, time.Now().UnixNano())
			lines = append(lines, line)
		}

		client := &http.Client{}
		limit := len(lines) / 2000 // 2K chunks are under 5K batch size recommendation for influx
		for j := 0; j < len(lines); j += limit {
			batch := lines[j:min(j+limit, len(lines))]
			body := strings.Join(batch, "\n")
			url := fmt.Sprintf("https://%s:%d/write?db=eos", influxHostname, influxPort)
			req, err := http.NewRequest("POST", url, strings.NewReader(body))
			if err != nil {
				er(err)
			}
			req.SetBasicAuth(influxUsername, influxPassword)
			res, err := client.Do(req)
			if err != nil {
				er(err)
			}

			if res.StatusCode != http.StatusNoContent {
				body, _ := ioutil.ReadAll(res.Body)
				errString := fmt.Sprintf("failed to write to influxdb: %v %v", res.StatusCode, string(body))
				err := errors.New(errString)
				er(err)
			}
		}

	},
}

var nsStatCmd = &cobra.Command{
	Use:   "eos-ns-stat",
	Short: "Retrieves NS operation calls per user",
	Run: func(cmd *cobra.Command, args []string) {
		influxUsername := viper.GetString("influx_username")
		influxPassword := viper.GetString("influx_password")
		influxHostname := viper.GetString("influx_hostname")
		influxPort := viper.GetInt("influx_port")
		type fourTuple struct {
			instance string
			username string
			op       string
			value    float64
		}
		tuples := []*fourTuple{}
		mgms := []string{"eoshome-i00", "eoshome-i01", "eoshome-i02", "eoshome-i03", "eoshome-i04", "eosproject-i00", "eosproject-i01", "eosproject-i02"}
		for _, i := range mgms {
			a := `eos -r 0 0 ns stat -m -a | grep cmd | sed 's/=/ /g' | grep -v 'root cmd' | sed 's/gid all //g' | awk '{print $2,$4,$14}'`
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
				if len(tokens) < 3 {
					continue
				}
				username := tokens[0]
				op := tokens[1]
				op = strings.ReplaceAll(op, "::", "-")
				hz60m := tokens[2]
				hz60mFloat, err := strconv.ParseFloat(hz60m, 64)
				if err != nil {
					continue
				}
				hz60mFloat = hz60mFloat * 60 * 60 // get total ops per hour

				tuples = append(tuples, &fourTuple{
					instance: i,
					username: username,
					op:       op,
					value:    hz60mFloat,
				})
			}

		}

		lines := []string{}
		for _, t := range tuples {
			line := fmt.Sprintf("nsops,instance=%s,op=%s,username=%s value=%f %d", t.instance, t.op, t.username, t.value, time.Now().UnixNano())
			lines = append(lines, line)
		}

		client := &http.Client{}
		limit := len(lines) / 2000 // 2K chunks are under 5K batch size recommendation for influx
		for j := 0; j < len(lines); j += limit {
			batch := lines[j:min(j+limit, len(lines))]
			body := strings.Join(batch, "\n")
			url := fmt.Sprintf("https://%s:%d/write?db=eos", influxHostname, influxPort)
			req, err := http.NewRequest("POST", url, strings.NewReader(body))
			if err != nil {
				er(err)
			}
			req.SetBasicAuth(influxUsername, influxPassword)
			res, err := client.Do(req)
			if err != nil {
				er(err)
			}

			if res.StatusCode != http.StatusNoContent {
				body, _ := ioutil.ReadAll(res.Body)
				errString := fmt.Sprintf("failed to write to influxdb: %v %v", res.StatusCode, string(body))
				err := errors.New(errString)
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

// filter out to keep only top 20 per op
type pair struct {
	user string
	hz   float64
}
type pairList []pair

func (p pairList) Len() int           { return len(p) }
func (p pairList) Less(i, j int) bool { return p[i].hz < p[j].hz }
func (p pairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
