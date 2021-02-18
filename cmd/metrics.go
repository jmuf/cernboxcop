package cmd

import (
	"bytes"
	"context"
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

type probeFun func(string, string, string, *error, *sync.WaitGroup)

type probe struct {
	Name        string
	User        string
	Password    string
	Func        probeFun
	Nodes       []string
	NodesFailed map[string]error
	IsSuccess   bool
}

var verbose bool

func Probe(name string, user string, password string, probeTest probeFun, nodes []string) probe {
	return probe{name, user, password, probeTest, nodes, make(map[string]error), true}
}

func (p *probe) GetListNodesFailed() []string {
	keys := []string{}
	for key := range p.NodesFailed {
		keys = append(keys, string(key))
	}
	return keys
}

func (p *probe) Run() {
	errors := make([]error, len(p.Nodes))
	var wg sync.WaitGroup

	for i, node := range p.Nodes {
		wg.Add(1)
		go p.Func(node, p.User, p.Password, &errors[i], &wg)
	}
	wg.Wait()

	for i, node := range p.Nodes {
		if errors[i] != nil {
			p.IsSuccess = false
			p.NodesFailed[node] = errors[i]
		}
	}
}

func (p *probe) PrintReport() {
	if p.IsSuccess {
		logSuccess(fmt.Sprintf("%s successfully runned\n", p.Name))
		return
	}

	// some errors in the test

	errorMsg := fmt.Sprintf("%s failed", p.Name)

	if len(p.NodesFailed) != 0 {
		errorMsg += " on the following nodes\n"
		for node, err := range p.NodesFailed {
			errorMsg += fmt.Sprintf("\t%s ->  %s\n", node, err)
		}
	}

	// print on stdout the error
	logError(errorMsg)

}

func logSuccess(str string) {
	fmt.Println("[SUCCESS]", str)
}

func logError(str string) {
	fmt.Println("[ERROR]", str)
}

func init() {
	//	nsStatCmd.Flags().StringP("carbon-server", "c", "filer-carbon.cern.ch:2003", "graphite server")
	//	nsStatCmd.Flags().StringP("prefix", "p", "test.cernbox.newmetrics3.eos.ns-stats", "namespace for metrics")

	rootCmd.AddCommand(metricsCmd)
	metricsCmd.AddCommand(availabilityCmd)
	metricsCmd.AddCommand(nsStatCmd)
	metricsCmd.AddCommand(ioStatCmd)
	metricsCmd.AddCommand(quotaCmd)

	availabilityCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "verbose output")
}

var metricsCmd = &cobra.Command{
	Use:   "metrics",
	Short: "CERNBox service metrics",
}

func webDavTest(node, user, password string, e *error, wg *sync.WaitGroup) {
	text := "dummy text with time " + time.Now().String()
	defer wg.Done()
	serverURL := fmt.Sprintf("https://cernbox.cern.ch/cernbox/desktop/remote.php/webdav/eos/user/%s/%s/sls", user[:1], user)
	httpClient := &http.Client{}

	// Create the remote folder
	mkdirReq, err := http.NewRequest("MKCOL", serverURL, nil)
	if err != nil {
		*e = err
		return
	}

	mkdirReq.SetBasicAuth(user, password)
	mkdirRes, err := httpClient.Do(mkdirReq)
	if err != nil {
		*e = err
		return
	}

	defer mkdirRes.Body.Close()
	if mkdirRes.StatusCode != http.StatusOK && mkdirRes.StatusCode != http.StatusCreated {
		*e = fmt.Errorf("MKCOL calls are failing")
		// sendStatus("degraded", "WebDAV MKCOL calls are failing")
		return
	}

	// Upload a file
	uploadReq, err := http.NewRequest("PUT", serverURL+"/dummy.txt", strings.NewReader(text))
	if err != nil {
		*e = err
		return
	}

	uploadReq.SetBasicAuth(user, password)
	uploadRes, err := httpClient.Do(uploadReq)
	if err != nil {
		*e = err
		return
	}

	defer uploadRes.Body.Close()
	if uploadRes.StatusCode != http.StatusOK && uploadRes.StatusCode != http.StatusCreated {
		fmt.Printf("in web dav %s", err)
		// sendStatus("degraded", "WebDAV uploads are failing")
		return
	}

	// Download the file
	downloadReq, err := http.NewRequest("GET", serverURL+"/dummy.txt", nil)
	if err != nil {
		*e = err
		return
	}

	downloadReq.SetBasicAuth(user, password)
	downloadRes, err := httpClient.Do(downloadReq)
	if err != nil {
		*e = err
		return
	}

	defer downloadRes.Body.Close()
	if downloadRes.StatusCode != http.StatusOK {
		*e = fmt.Errorf("downloads are failing")
		// sendStatus("degraded", "WebDAV downloads are failing")
		return
	}
	body, err := ioutil.ReadAll(downloadRes.Body)
	if err != nil {
		*e = err
		return
	}

	if string(body) != text {
		*e = fmt.Errorf("downloads are failing")
		// sendStatus("degraded", "WebDAV downloads are failing")
		return
	}
}

var availabilityCmd = &cobra.Command{
	Use:   "availability",
	Short: "Checks the CERNBox HTTP service and EOS instances for availability",
	Run: func(cmd *cobra.Command, args []string) {

		// initialization
		user, password := getProbeUser()
		if user == "" || password == "" {
			er("please set probe_user and probe_password in the config")
		}

		mgmsACLs := getProbeACLsInstances()
		mgmsXrdcp := getProbeXrdcpInstances()
		pathEosFuse := getProbeEosPath()

		// Define all tests
		probeTests := []probe{
			Probe("WebDAV", user, password, webDavTest, []string{"cernbox.cern.ch"}),
			Probe("ListACLs", user, "", aclTest, mgmsACLs),
			Probe("Xrdcp", user, "", xrdcpTest, mgmsXrdcp),
			Probe("Fuse EOS", "", "", eosFuseTest, pathEosFuse),
		}

		// run tests
		for i := range probeTests {
			probeTests[i].Run()
			probeTests[i].PrintReport()
		}

		SendStatus(&probeTests)

	},
}

func aclTest(node, user, password string, e *error, wg *sync.WaitGroup) {
	defer wg.Done()
	eosClient := getEOS(fmt.Sprintf("root://%s.cern.ch", node))
	path := fmt.Sprintf("/eos/%s/opstest/sls", strings.TrimPrefix(node, "eos"))

	ctx := getCtx()

	_, err := eosClient.ListACLs(ctx, user, path)
	if err != nil {
		*e = err
		return
	}
}

func eosFuseTest(path, user, password string, e *error, wg *sync.WaitGroup) {
	defer wg.Done()

	cmd := "ls " + path
	cmdBash := exec.Command("/usr/bin/bash", "-c", cmd)
	ctx, _ := context.WithTimeout(context.Background(), time.Second*30)
	_, _, *e = execute(ctx, cmdBash)
}

func xrdcpTest(node, user, password string, e *error, wg *sync.WaitGroup) {
	defer wg.Done()
	eosClient := getEOS(fmt.Sprintf("root://%s.cern.ch", node))

	text := "dummy text with time " + time.Now().String()
	reader := strings.NewReader(text)
	ctx := getCtx()

	if err := eosClient.Write(ctx, user, fmt.Sprintf("/eos/%s/opstest/sls/dummy.txt", strings.TrimPrefix(node, "eos")), ioutil.NopCloser(reader)); err != nil {
		*e = err
		return
	}

	if body, err := eosClient.Read(ctx, user, fmt.Sprintf("/eos/%s/opstest/sls/dummy.txt", strings.TrimPrefix(node, "eos"))); err != nil {
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
