package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cs3org/reva/pkg/eosclient"
	"github.com/dustin/go-humanize"
	"github.com/leekchan/accounting"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tj/go-spin"
	"gopkg.in/ldap.v3"
	"io/ioutil"
	"net/http"
	"os"
	"os/user"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const FE = "CERNBox"

func init() {
	rootCmd.AddCommand(accountingCmd)
	accountingCmd.AddCommand(accountingReportCmd)

	accountingReportCmd.Flags().IntP("concurrency", "c", 200, "use up to <n> concurrent connections to retrive information from external services (LDAP)")
	accountingReportCmd.Flags().IntP("limit", "l", -1, "reports for <n> first projects and <n> first users. -1 means all.")
	accountingReportCmd.Flags().Bool("charging", false, "obtains charging information from account receiver")
	accountingReportCmd.Flags().Bool("user-also", false, "computes user home directories also")
	accountingReportCmd.Flags().Bool("show-invalid", false, "shows projects with invalid information, to be archived/retired because missing user information")
	accountingReportCmd.Flags().Bool("push-dev", false, "push data to acc-receiver-dev.cern.ch")
	accountingReportCmd.Flags().Bool("push-eos", false, "store data into /eos/project/f/fdo/www/accounting/data")
	accountingReportCmd.Flags().Bool("push-prod", false, "push data to acc-receiver.cern.ch")
	accountingReportCmd.Flags().Bool("as-yesterday", false, "useful when computing metrics from previous day. Use when pushing to API after midnight")
	accountingReportCmd.Flags().StringP("out", "o", ".", "directory to output accounting information")
	accountingReportCmd.Flags().Float64P("cost", "", 2.20, "cost factor for CHF/TBMonth")
}

var accountingCmd = &cobra.Command{
	Use:   "accounting",
	Short: "Accounting for CERNBox EOS instances",
}

var accountingReportCmd = &cobra.Command{
	Use:   "report",
	Short: "Reports all project information",
	Run: func(cmd *cobra.Command, args []string) {
		head, _ := cmd.Flags().GetInt("limit")
		conc, _ := cmd.Flags().GetInt("concurrency")
		userAlso, _ := cmd.Flags().GetBool("user-also")
		showInvalid, _ := cmd.Flags().GetBool("show-invalid")
		pushProd, _ := cmd.Flags().GetBool("push-prod")
		pushDev, _ := cmd.Flags().GetBool("push-dev")
		pushEOS, _ := cmd.Flags().GetBool("push-eos")
		asYesterday, _ := cmd.Flags().GetBool("as-yesterday")
		out, _ := cmd.Flags().GetString("out")
		factorPerTB, _ := cmd.Flags().GetFloat64("cost")
		var factorPerByte float64 = factorPerTB / float64(1000000000000)

		infos := getEOSProjects(head)
		if userAlso {
			infos = append(infos, getEOSUsers(head)...)
		}

		l := getLDAP()
		defer l.Close()
		userInfos := getUserInfos(l, infos, conc)
		fillUserInfos(infos, userInfos)

		instances := getInstances(infos)
		quotas := getQuotas(instances...)
		fillQuotas(infos, quotas)

		charge, _ := cmd.Flags().GetBool("charging")
		if charge {
			charges := getCharging(infos, conc)
			fillCharging(infos, charges)
			fillChargeRoles(infos)
			infos = cleanInfos(infos, showInvalid)
		}

		fmt.Fprintln(os.Stderr)

		file := path.Join(out, "accounting.txt")
		files := []string{file} // all files that are going to be generated
		computeBasic(infos, file, factorPerByte)
		fmt.Printf("%s\n", file)
		if charge {
			file := path.Join(out, "accounting-agg-groups.txt")
			files = append(files, file)
			computeAggregateToGroups(infos, path.Join(out, "accounting-agg-groups.txt"), factorPerByte)
			fmt.Printf("%s\n", file)

			file = path.Join(out, "accounting-agg.txt")
			files = append(files, file)
			computeAggregate(infos, file, factorPerByte)
			fmt.Printf("%s\n", file)

			file = path.Join(out, "accounting-agg-simple.txt")
			files = append(files, file)
			computeAggregateSimplified(infos, file, factorPerByte)
			fmt.Printf("%s\n", file)

			file = path.Join(out, "accounting-json-accreceiver.json")
			files = append(files, file)
			computeAggregateReceiverJSON(infos, file, asYesterday)
			fmt.Printf("%s\n", file)

			//  curl -X POST -H "Content-Type: application/json" -H "API-Key:xyz"  https://acc-receiver-dev.cern.ch/v3/fe
			if pushProd {
				url := "https://acc-receiver.cern.ch/v3/fe"
				pushData(url, file)
				fmt.Println("Data pushed to " + url)
			} else if pushDev {
				url := "https://acc-receiver-dev.cern.ch/v3/fe"
				pushData(url, file)
				fmt.Println("Data pushed to " + url)
			}

		}
		if pushEOS {
			saveToEOS(files...)
		}
	},
}

// storage files into cernbox project in EOS
var saveToEOS = func(files ...string) {
	ctx := getCtx()
	client := getEOS("root://eosproject-f.cern.ch")
	key := time.Now().Local().Format("2006/01/02")
	ctx, _ = context.WithTimeout(ctx, time.Second*30)
	dir := "/eos/project/f/fdo/www/accounting/data/cernbox/"
	err := client.CreateDir(ctx, "ml001", path.Join(dir, key))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error creating accounting directory in EOS: %+v\n", err)
		er(err)
	}

	// save files
	for _, f := range files {
		fd, err := os.Open(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error reading file: %+v\n", err)
			er(err)

		}
		defer fd.Close()
		name := path.Join(dir, key, path.Base(f))
		err = client.Write(ctx, "ml001", name, fd)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error writing file: %+v\n", err)
			er(err)
		}
	}

}

var timeNow = func(asYesterday bool) time.Time {
	t := time.Now().Local()
	if asYesterday {
		t = t.Add(-time.Hour * 24)
	}
	return t
}

var pushData = func(endpoint, file string) {
	client := &http.Client{}
	data, err := ioutil.ReadFile(file)
	if err != nil {

		log.Error().Msgf("error pushing data to:%s err:%+v", endpoint, file, err)
		er(err)
	}
	req, err := http.NewRequest("POST", endpoint, strings.NewReader(string(data)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Length", fmt.Sprintf("%d", len(data)))
	key := viper.GetString("receiver_api_key")
	req.Header.Set("API-Key", key)
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error pushing data to account receiver:%s %+v\n", endpoint, err)
		er(err)
	}
	defer resp.Body.Close()
	result, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "error pushing data to account receiver:%s %+v\n", endpoint, string(result))

	}
	log.Info().Msgf("Results from pushing data to receiver service: %s\n", string(result))

}

var getCost = func(bytes int, factorCost float64) string {
	ac := accounting.Accounting{Symbol: "CHF", Precision: 2}
	return ac.FormatMoney(factorCost * float64(bytes))
}

var computeAggregateReceiverJSON = func(infos []*projectInfo, file string, asYesterday bool) {
	infos = uniqueInfos(infos)
	aggregate := map[string]map[string]eosclient.QuotaInfo{}
	for _, info := range infos {
		group := info.chargeInfo.ChargeGroup
		role := info.chargeInfo.ChargeRole
		roles, ok := aggregate[group]
		if !ok {
			aggregate[group] = map[string]eosclient.QuotaInfo{
				role: *info.QuotaInfo,
			}
		}

		// here we have the group, we don't know if we have the role
		if _, ok := roles[role]; !ok {
			aggregate[group][role] = *info.QuotaInfo

		}

		// at this point we are sure we have the group and the role
		quota, _ := aggregate[group][role]
		quota.AvailableBytes += info.QuotaInfo.AvailableBytes
		quota.UsedBytes += info.QuotaInfo.UsedBytes

	}

	payload_data := []*accReceiverJSON_v3_data{}
	for group, roles := range aggregate {
		for role, quota := range roles {

			j := &accReceiverJSON_v3_data{
				ToChargeGroup: group,
				MetricValue:   quota.UsedBytes,
				ToChargeRole:  role,
			}
			payload_data = append(payload_data, j)
		}
	}
	payload := &accReceiverJSON_v3_header{
		MessageFormatVersion: 3,
		FromChargeGroup:      FE,
		MetricName:           "UsedBytes",
		TimePeriod:           "day",
		TimeStamp:            timeNow(asYesterday).Format("2006-01-02"),
		TimeAggregate:        "avg",
		AccountingDoc:        "CERNBox accounts on actually-used space in EOS",
		Data:                 payload_data,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		er(err)
	}

	saveWith(file, data)
}
var computeBasic = func(infos []*projectInfo, file string, costFactor float64) {
	cols := []string{
		"UID",
		"GID",
		"INSTANCE",
		"PATH",
		"MAXBYTES",
		"USEDBYTES",
		"MAXBYTESH",
		"USEDBYTESH",
		"CREATED",
		"ACCTYPE",
		"ACC",
		"OWNER",
		"NAME",
		"DEPT",
		"GROUP",
		"SECTION",
		"CHARGETYPE",
		"CHARGEGROUP",
		"CHARGEROLE",
		"COSTH",
	}

	rows := [][]string{}
	for _, p := range infos {
		row := []string{
			p.userInfo.AccountOwner.UID,
			p.userInfo.AccountOwner.GID,
			p.FileInfo.Instance,
			p.FileInfo.File,
			fmt.Sprintf("%d", p.QuotaInfo.AvailableBytes),
			fmt.Sprintf("%d", p.QuotaInfo.UsedBytes),
			humanQuota(p.QuotaInfo.AvailableBytes),
			humanQuota(p.QuotaInfo.UsedBytes),
			p.CreatedHuman(),
			p.userInfo.accountTypeHuman(),
			p.userInfo.Account,
			p.userInfo.AccountOwner.Account,
			p.userInfo.AccountOwner.Name,
			p.userInfo.AccountOwner.Department,
			p.userInfo.AccountOwner.Group,
			p.userInfo.AccountOwner.Section,
			p.chargeInfo.Type,
			p.chargeInfo.ChargeGroup,
			p.chargeInfo.ChargeRole,
			getCost(p.QuotaInfo.UsedBytes, costFactor),
		}
		rows = append(rows, row)
	}

	save(cols, rows, file)
}

var humanQuota = func(bytes int) string {
	return humanize.Bytes(uint64(bytes))
}

var cleanInfos = func(infos []*projectInfo, showInvalid bool) []*projectInfo {
	if showInvalid {
		return infos
	}

	// clean
	clean := make([]*projectInfo, 0, len(infos))
	for _, info := range infos {
		if info.chargeInfo.ChargeGroup != "" && info.chargeInfo.ChargeGroup != "Unknown" {
			clean = append(clean, info)
		}
	}
	return clean
}

var computeAggregateToGroups = func(infos []*projectInfo, file string, costFactor float64) {
	infos = uniqueInfos(infos)
	aggregate := map[string]eosclient.QuotaInfo{}
	for _, info := range infos {
		group := info.chargeInfo.ChargeGroup
		quota, ok := aggregate[group]
		if !ok {
			aggregate[group] = *info.QuotaInfo

		}

		quota = aggregate[group]
		quota.AvailableBytes += info.QuotaInfo.AvailableBytes
		quota.UsedBytes += info.QuotaInfo.UsedBytes
		aggregate[group] = quota

	}

	// sort by charge groups
	keys := make([]string, 0, len(aggregate))
	for k := range aggregate {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	cols := []string{
		"MAXBYTES",
		"USEDBYTES",
		"MAXBYTESH",
		"USEDBYTESH",
		"CREATED",
		"CHARGEGROUP",
		"COSTH",
	}

	rows := [][]string{}
	for _, group := range keys {
		quota, _ := aggregate[group]
		created := time.Now().Local().Format("2006-01-02")
		row := []string{
			fmt.Sprintf("%d", quota.AvailableBytes),
			fmt.Sprintf("%d", quota.UsedBytes),
			humanQuota(quota.AvailableBytes),
			humanQuota(quota.UsedBytes),
			created,
			group,
			getCost(quota.UsedBytes, costFactor),
		}
		rows = append(rows, row)
	}

	save(cols, rows, file)
}

var uniqueInfos = func(infos []*projectInfo) (uniq []*projectInfo) {
	keys := map[string]*projectInfo{}
	for _, info := range infos {
		key := fmt.Sprintf("%s-%d-%d", info.Account, info.QuotaInfo.AvailableBytes, info.QuotaInfo.UsedBytes)
		keys[key] = info
	}

	for _, info := range keys {
		uniq = append(uniq, info)
	}

	return
}

var computeAggregateSimplified = func(infos []*projectInfo, file string, costFactor float64) {
	// remove duplicate quota entries
	infos = uniqueInfos(infos)
	aggregate := map[string]map[string]eosclient.QuotaInfo{}
	for _, info := range infos {
		group := info.chargeInfo.ChargeGroup
		role := info.chargeInfo.ChargeRole
		if strings.HasPrefix(role, "CERNBox Project") {
			role = "CERNBox Project Spaces"
		} else if strings.Contains(role, "Account") {
			role = "CERNBox Home Directories"
		} else {
			role = "Unknown"
		}

		roles, ok := aggregate[group]
		if !ok {
			aggregate[group] = map[string]eosclient.QuotaInfo{
				role: *info.QuotaInfo,
			}
		}

		// here we have the group, we don't know if we have the role
		if _, ok := roles[role]; !ok {
			aggregate[group][role] = *info.QuotaInfo

		}

		// at this point we are sure we have the group and the role
		quota, _ := aggregate[group][role]
		quota.AvailableBytes += info.QuotaInfo.AvailableBytes
		quota.UsedBytes += info.QuotaInfo.UsedBytes
		aggregate[group][role] = quota

	}

	// sort by charge groups
	keys := make([]string, 0, len(aggregate))
	for k := range aggregate {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	cols := []string{
		"MAXBYTES",
		"USEDBYTES",
		"MAXBYTESH",
		"USEDBYTESH",
		"CREATED",
		"CHARGEGROUP",
		"CHARGEROLE",
		"COSTH",
	}

	rows := [][]string{}
	for _, group := range keys {
		roles, _ := aggregate[group]
		keys := make([]string, 0, len(roles))
		for k := range roles {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, role := range keys {
			quota, _ := roles[role]
			created := time.Now().Local().Format("2006-01-02")
			row := []string{
				fmt.Sprintf("%d", quota.AvailableBytes),
				fmt.Sprintf("%d", quota.UsedBytes),
				humanQuota(quota.AvailableBytes),
				humanQuota(quota.UsedBytes),
				created,
				group,
				role,
				getCost(quota.UsedBytes, costFactor),
			}
			rows = append(rows, row)
		}
	}

	save(cols, rows, file)

}
var computeAggregate = func(infos []*projectInfo, file string, costFactor float64) {
	aggregate := map[string]map[string]eosclient.QuotaInfo{}
	for _, info := range infos {
		group := info.chargeInfo.ChargeGroup
		role := info.chargeInfo.ChargeRole
		roles, ok := aggregate[group]
		if !ok {
			aggregate[group] = map[string]eosclient.QuotaInfo{
				role: *info.QuotaInfo,
			}
		}

		// here we have the group, we don't know if we have the role
		if _, ok := roles[role]; !ok {
			aggregate[group][role] = *info.QuotaInfo

		}

		// at this point we are sure we have the group and the role
		quota, _ := aggregate[group][role]
		quota.AvailableBytes += info.QuotaInfo.AvailableBytes
		quota.UsedBytes += info.QuotaInfo.UsedBytes

	}

	// sort by charge groups
	keys := make([]string, 0, len(aggregate))
	for k := range aggregate {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	cols := []string{
		"MAXBYTES",
		"USEDBYTES",
		"MAXBYTESH",
		"USEDBYTESH",
		"CREATED",
		"CHARGEGROUP",
		"CHARGEROLE",
		"COSTH",
	}

	rows := [][]string{}
	for _, group := range keys {
		roles, _ := aggregate[group]
		keys := make([]string, 0, len(roles))
		for k := range roles {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, role := range keys {
			quota, _ := roles[role]
			created := time.Now().Local().Format("2006-01-02")
			row := []string{
				fmt.Sprintf("%d", quota.AvailableBytes),
				fmt.Sprintf("%d", quota.UsedBytes),
				humanQuota(quota.AvailableBytes),
				humanQuota(quota.UsedBytes),
				created,
				group,
				role,
				getCost(quota.UsedBytes, costFactor),
			}
			rows = append(rows, row)
		}
	}

	save(cols, rows, file)

}

// cleans roles and groups
var fillChargeRoles = func(infos []*projectInfo) {
	for _, info := range infos {
		if strings.HasPrefix(info.FileInfo.File, "/eos/project/") {
			info.chargeInfo.ChargeRole = "CERNBox Project " + path.Base(info.FileInfo.File)
		} else {
			info.chargeInfo.ChargeRole = info.userInfo.accountTypeHuman()
		}

		if info.chargeInfo.ChargeRole == "" {
			info.chargeInfo.ChargeRole = "Unknown"
		}

		if info.chargeInfo.ChargeGroup == "" {
			info.chargeInfo.ChargeGroup = "Unknown"
		}

	}
}

var getUserInfos = func(lc *ldap.Conn, infos []*projectInfo, concurrency int) map[uint64]*userInfo {
	var throttle = make(chan int, concurrency)
	var wg sync.WaitGroup

	s := spin.New()
	m := make(map[uint64]*userInfo, len(infos))
	l := len(infos)
	mux := sync.Mutex{}
	for i, p := range infos {
		throttle <- 1 // whatever number
		wg.Add(1)

		go func(i int, s *spin.Spinner, p *projectInfo, wg *sync.WaitGroup, throttle chan int) {
			defer wg.Done()
			defer func() {
				<-throttle
			}()

			ui := getUserInfo(lc, p.FileInfo.UID)
			mux.Lock()
			defer mux.Unlock()
			m[p.FileInfo.UID] = ui
			fmt.Fprintf(os.Stderr, "\r %s Getting account info [%d/%d]", s.Next(), i, l)
		}(i, s, p, &wg, throttle)
	}
	fmt.Fprintln(os.Stderr)
	wg.Wait()
	return m
}

var getUserInfo = func(lc *ldap.Conn, uid uint64) *userInfo {
	username, err := getUsername(uid)
	if err != nil {
		// we don't fill user info
		return newUserInfo()
	}

	ui := getUserFull(lc, username)
	return ui

}

var getInstances = func(infos []*projectInfo) []string {
	uniq := map[string]interface{}{}
	s := spin.New()
	for _, info := range infos {
		uniq[info.FileInfo.Instance] = nil
		fmt.Fprintf(os.Stderr, "\r %s Getting EOS instances names: %s", s.Next(), info.FileInfo.Instance)
	}
	fmt.Fprintln(os.Stderr)

	instances := make([]string, 0, len(infos))
	for k := range uniq {
		instances = append(instances, k)
	}
	return instances

}

var getCharging = func(infos []*projectInfo, concurrency int) map[string]*chargeInfo {
	// obtain list of usernames
	// and send them in a big JSON document
	accounts := make([]string, 0, len(infos))
	for _, v := range infos {
		accounts = append(accounts, v.userInfo.Account)
	}

	// chunk requests so we don't get gateway timeouts
	chunks := [][]string{}
	chunkSize := 1000
	for i := 0; i < len(accounts); i += chunkSize {
		end := i + chunkSize

		if end > len(accounts) {
			end = len(accounts)
		}

		chunks = append(chunks, accounts[i:end])
	}

	charges := map[string]*chargeInfo{}
	mux := sync.Mutex{}

	var throttle = make(chan int, 1)
	var wg sync.WaitGroup
	url := "https://accounting-receiver.cern.ch/v2/"
	s := spin.New()
	counter := uint64(0)
	totalAccounts := len(accounts)
	for _, accounts := range chunks {
		throttle <- 1 // whatever number
		wg.Add(1)
		go func(accounts []string, wg *sync.WaitGroup, throttle chan int) {
			client := &http.Client{}
			atomic.AddUint64(&counter, uint64(len(accounts)))
			defer wg.Done()
			defer func() {
				<-throttle
			}()
			ch := &chargeJSON{Users: accounts}
			body, err := json.Marshal(ch)
			if err != nil {
				er(err)
			}

			req, err := http.NewRequest("GET", url, strings.NewReader(string(body)))
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error GETing account receiver: %+v", err)
				er(err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				er(fmt.Sprintf("error GETing account received, HTTP error code: %+v", resp.StatusCode))
			}

			body, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				er(err)
			}
			cr := chargeResponse{}
			if err := json.Unmarshal(body, &cr); err != nil {
				log.Error().Msgf("error parsing account receiver: %+v", err)
				er(err)
			}

			log.Info().Msgf("Charge info: sent:%d got:%d")
			fmt.Fprintf(os.Stderr, "\r %s Resolving charging information [%d/%d]", s.Next(), counter, totalAccounts)
			for k, v := range cr {
				ci := &chargeInfo{}
				if err := mapstructure.Decode(v, ci); err != nil {
					log.Error().Msgf("error decoding: account:%s value:%s", k, v)
					continue
				}
				// validate input
				// TODO(labkode): report that the API returns charge type with whitespaces at the beggining.
				ci.Type = strings.TrimSpace(ci.Type)
				ci.ChargeGroup = strings.TrimSpace(ci.ChargeGroup)
				log.Info().Msgf("charge info for account: %s %+v", k, ci)

				mux.Lock()
				charges[k] = ci
				mux.Unlock()
			}

		}(accounts, &wg, throttle)
	}
	wg.Wait()
	return charges
}

type chargeJSON struct {
	Users []string `json:"users"`
}

type chargeResponse map[string]interface{}

var fillCharging = func(infos []*projectInfo, charges map[string]*chargeInfo) {
	s := spin.New()
	count := len(infos)
	for i, info := range infos {
		if v, ok := charges[info.userInfo.Account]; ok {
			info.chargeInfo = *v
		}

		fmt.Fprintf(os.Stderr, "\r %s Filling charge info [%d/%d]", s.Next(), i, count)
	}

	return
}

var fillQuotas = func(infos []*projectInfo, quotas map[string]*eosclient.QuotaInfo) {
	s := spin.New()
	count := len(infos)
	for i, p := range infos {
		if p.userInfo.Account != "" {
			k := p.userInfo.Account + p.FileInfo.Instance
			if v, ok := quotas[k]; ok {
				p.QuotaInfo = v
			}
		}
		fmt.Fprintf(os.Stderr, "\r %s Computing quota [%d/%d]", s.Next(), i, count)
	}
	return
}

var fillUserInfos = func(infos []*projectInfo, uis map[uint64]*userInfo) {
	s := spin.New()
	count := len(infos)
	for i, p := range infos {
		p.userInfo = uis[p.FileInfo.UID]
		fmt.Fprintf(os.Stderr, "\r %s Filling user info [%d/%d]", s.Next(), i, count)
	}
	fmt.Fprintln(os.Stderr)
	return
}

var getUsername = func(uid uint64) (string, error) {
	uidstr := fmt.Sprintf("%d", uid)
	u, err := user.LookupId(uidstr)
	if err != nil {
		return "", err
	}
	return u.Username, nil
}

var getEOSUsers = func(limit int) (infos []*projectInfo) {
	ctx := getCtx()
	var mds []*eosclient.FileInfo
	letters := "abcdefghijklmnopqrstuvwxyz"
	s := spin.New()
	for i := 0; i < len(letters); i++ {
		letter := string(letters[i])
		fmt.Fprintf(os.Stderr, "\r %s Getting users [%s]", s.Next(), letter)
		host := fmt.Sprintf("root://eoshome-%s.cern.ch", letter)
		client := getEOS(host)
		ctx, _ := context.WithTimeout(ctx, time.Second*30)
		m, err := client.List(ctx, "root", "/eos/user/"+letter)
		if err != nil {
			er(err)
		}
		mds = append(mds, m...)
	}
	fmt.Fprintln(os.Stderr)

	if limit == -1 {
		limit = len(mds)
	}

	for i, m := range mds {
		if i > limit {
			break
		}

		// ctime
		ctimestr := strings.Split(m.Attrs["ctime"], ".")[0]
		ctime, _ := strconv.ParseInt(ctimestr, 10, 64)
		t := time.Unix(ctime, 0)

		pi := &projectInfo{FileInfo: m, userInfo: newUserInfo(), QuotaInfo: &eosclient.QuotaInfo{}, chargeInfo: chargeInfo{}, created: t}
		infos = append(infos, pi)
	}

	return
}
var getEOSProjects = func(limit int) (infos []*projectInfo) {
	ctx := getCtx()
	var mds []*eosclient.FileInfo
	letters := "abcdefghijklmnopqrstuvwxyz"
	s := spin.New()
	for i := 0; i < len(letters); i++ {
		letter := string(letters[i])
		fmt.Fprintf(os.Stderr, "\r %s Getting project names [%s]", s.Next(), letter)
		host := fmt.Sprintf("root://eosproject-%s.cern.ch", letter)
		client := getEOS(host)
		ctx, _ := context.WithTimeout(ctx, time.Second*30)
		m, err := client.List(ctx, "root", "/eos/project/"+letter)
		if err != nil {
			er(err)
		}
		mds = append(mds, m...)
	}
	fmt.Fprintln(os.Stderr)

	if limit == -1 {
		limit = len(mds)
	}

	for i, m := range mds {
		if i > limit {
			break
		}

		// ctime
		ctimestr := strings.Split(m.Attrs["ctime"], ".")[0]
		ctime, _ := strconv.ParseInt(ctimestr, 10, 64)
		t := time.Unix(ctime, 0)

		pi := &projectInfo{FileInfo: m, userInfo: newUserInfo(), QuotaInfo: &eosclient.QuotaInfo{}, chargeInfo: chargeInfo{}, created: t}
		infos = append(infos, pi)
	}

	return
}

type projectInfo struct {
	created time.Time
	*eosclient.FileInfo
	*userInfo
	*eosclient.QuotaInfo
	toBeArchived bool
	chargeInfo
}

func (pi *projectInfo) CreatedHuman() string {
	return pi.created.Format("02/01/2006")
}

type chargeInfo struct {
	Type        string `json:"type" mapstructure:"type"`
	Owner       string `json:"owner" mapstructure:"owner"`
	ChargeGroup string `json:"charge_group" mapstructure:"charge_group"`
	ChargeRole  string `json:"charge_role" mapstructure:"charge_role"`
}

type chargeInfoSchema map[string]*chargeInfo

func getQuotas(mgms ...string) map[string]*eosclient.QuotaInfo {
	quotas := map[string]*eosclient.QuotaInfo{}
	s := spin.New()
	for _, mgm := range mgms {
		fmt.Fprintf(os.Stderr, "\r %s Getting quota for instance: %s", s.Next(), mgm)
		ctx, _ := context.WithTimeout(getCtx(), time.Second*60)
		eos := getEOS(mgm)
		prefix := "/eos/project/"
		if strings.Contains(mgm, "home") {
			prefix = "/eos/user/"
		}
		qts, err := eos.DumpQuotas(ctx, prefix)
		if err != nil {
			er(err)
		}
		for k, v := range qts {
			k += mgm
			quotas[k] = v
		}
	}
	return quotas
}

/* Format: see https://accounting-docs.web.cern.ch/services/v3/accounting/ */
type accReceiverJSON_v3_header struct {
	MessageFormatVersion int
	FromChargeGroup      string
	MetricName           string
	TimePeriod           string
	TimeStamp            string
	TimeAggregate        string
	AccountingDoc        string
	Data                 []*accReceiverJSON_v3_data `json:"data"`
}
type accReceiverJSON_v3_data struct {
	ToChargeGroup string
	MetricValue   int
	ToChargeRole  string
}
