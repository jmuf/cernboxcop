package cmd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cs3org/reva/pkg/eosclient"
	"github.com/spf13/cobra"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

func init() {
	rootCmd.AddCommand(ooCmd)
	ooCmd.Flags().BoolP("skip-recall", "s", false, "skips recall file, useful for testing logs file only")
}

var ooCmd = &cobra.Command{
	Use:   "oo <files_to_recall> <http_error_log>",
	Short: "Check if OO cache files need to be transfered to CERNBox",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 2 {
			exit(cmd)
		}

		// check both files exists
		for i := 0; i < 2; i++ {
			if _, err := os.Stat(args[i]); os.IsNotExist(err) {
				err := errors.New("file does not exists:" + args[0])
				er(err)
			}
		}

		// cache file is obtained running this command on
		// OO servers:
		// find . -type f -exec stat  -c "%Y %n" {} \;
		// 1604849880 /var/lib/onlyoffice/documentserver/App_Data/cache/files/eoshome-a.62904145.1604849861_9152/changesHistory.json
		// 1604849880 /var/lib/onlyoffice/documentserver/App_Data/cache/files/eoshome-a.62904145.1604849861_9152/changes.zip
		// 1604670405 /var/lib/onlyoffice/documentserver/App_Data/cache/files/eoshome-o.40421278.1604669416_2352/output.xlsx

		// read file
		data, err := ioutil.ReadFile(args[0])
		if err != nil {
			er(err)
		}

		forgotten := map[string]bool{}
		forgottenPaths := map[string]bool{}
		invalid := []string{}
		invalidEos := []string{}
		invalidSkip := map[string]*eosclient.FileInfo{}
		invalidSkipPath := []string{}
		seenKeys := map[string]bool{}
		records := []*record{}

		skipRecall, _ := cmd.Flags().GetBool("skip-recall")
		// split by newline
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			if skipRecall {
				break
			}
			// skip empty lines
			if line == "" {
				continue
			}

			// split into epoch and path
			tokens := strings.Split(line, " ")
			if len(tokens) < 2 {
				invalid = append(invalid, line)
				continue
			}

			epochString := tokens[0]
			path := tokens[1]

			// convert epoch to time
			epoch, err := epochStringToTime(epochString)
			if err != nil {
				invalid = append(invalid, line)
				continue
			}

			// parse path to transform
			// from: /var/lib/onlyoffice/documentserver/App_Data/cache/files/eoshome-o.40421278.1604669416_2352/output.xlsx
			// to: eoshome-o.40421278.1604669416_2352
			// Often time we get keys like: eoshome-a.198150123040014336_9708
			// so we need to trim the "_9708"
			var key string
			var forgot bool // means file was in OO forgotten area (not saved back to EOS)
			if strings.HasPrefix(path, "/var/lib/onlyoffice/documentserver/App_Data/cache/files/forgotten/") {
				key = strings.TrimPrefix(path, "/var/lib/onlyoffice/documentserver/App_Data/cache/files/forgotten/")
				forgot = true
			} else {
				key = strings.TrimPrefix(path, "/var/lib/onlyoffice/documentserver/App_Data/cache/files/")
			}
			key = strings.Split(key, "/")[0]
			key = strings.Split(key, "_")[0]

			// a key can appear in the forgotten area or in a normal area.
			// forgotten area takes precedence to be on the safe side to always
			// check for the status on EOS
			if seenKeys[key] && !forgot {
				continue
			}

			seenKeys[key] = true
			if forgot {
				forgotten[key] = true
			}
			// key is:
			// eoshome-o.40421278.1604669416_2352

			tokens = strings.Split(key, ".")
			if len(tokens) < 2 {
				invalid = append(invalid, line)
				continue
			}

			instance := tokens[0]
			instance = strings.ReplaceAll(instance, "newproject", "eosproject")
			inodeString := tokens[1]
			inode, err := strconv.ParseUint(inodeString, 10, 64)
			if err != nil {
				invalid = append(invalid, line)
				continue
			}

			mgm := fmt.Sprintf("root://%s.cern.ch", instance)
			client := getEOS(mgm)
			ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
			fi, err := client.GetFileInfoByInode(ctx, "root", inode)
			if err != nil {
				k := fmt.Sprintf("forgotten=%t line=%s", forgotten[key], line)
				invalidEos = append(invalidEos, k)
				continue
			}

			eosPath := fi.File
			currentEpoch := time.Unix(int64(fi.MTimeSec), 0)

			if forgotten[key] {
				forgottenPaths[eosPath] = true
			}

			// check if eosPath points to nominal value,
			// skipping versions and trashbin areas
			if strings.Contains(eosPath, ".sys.v#") || strings.Contains(eosPath, "proc/recycle") {
				k := fmt.Sprintf("forgotten=%t line=%s eos=%s", forgotten[key], line, eosPath)
				invalidSkipPath = append(invalidSkipPath, k)
				continue
			}

			// if the current mtime is newer than the one in OO cache
			// we skip as it means the file was overwritten in place
			// fusex or samba. Other  update methods (web, sync) creates a
			// version file and is covered in previous step

			if currentEpoch.After(epoch) {
				k := fmt.Sprintf("forgotten=%t line=%s", forgotten[key], line)
				invalidSkip[k] = fi
				continue
			}

			// we are left with files that have a bigger mtime in OO that in EOS.
			// This does not mean that files never reached EOS, bacause this list also contains
			// files that were downloaded to OO cache (therefore mtime is greater than in EOS).
			// To exclude these files we can analyze the ocproxy logs and retrieve POST requests that
			// failed with HTTP error code 500. Based on the timestamp of the request we can determine
			// if the file lying on OO was created for reading or for writing.
			// If it was for reading, we can skip.

			// fmt.Printf("forgotten=%t In OO: %s (%v)  In EOS: %s (%v)\n", forgotten[key], key, epoch, fi.File, currentEpoch)

			r := &record{
				line:         line,
				key:          key,
				epochOO:      epoch,
				currentEpoch: currentEpoch,
				fi:           fi,
			}
			records = append(records, r)
		}

		// process the log file
		// the logs file contains the failed POST requests from OO to CERNBox. The file is obtained with this command:
		// [14:54][root@cbox-grimoire-02 (qa:box/grimoire) ~]# grep -h onlyoffice /data/log/2020/11/09/box.lbweb/cbox-lbweb*/*ocproxy_http* | grep POST | grep 'storage/track' |  grep '"code":500,"' > /var/tmp/500s
		data, err = ioutil.ReadFile(args[1])
		if err != nil {
			er(err)
		}

		eosPaths := map[string]bool{}
		// split by line
		ll := strings.Split(string(data), "\n")
		for _, l := range ll {
			// skip empty lines
			if l == "" {
				continue
			}

			// parse json
			jl := &jsonLine{}
			if err := json.Unmarshal([]byte(l), jl); err != nil {
				er(err)
			}

			// line is:
			// {
			//   "host": "127.0.0.1",
			//   "user": null,
			//   "method": "POST",
			//   "path": "/index.php/apps/onlyoffice/storage/track/__myprojects/lhc-experimental-beam-vacuum/ATLAS%20Experimental%20Vacuum/Long%20Shutdown%202%20-%20ongoing%20work%20tasks/01%20Design%20&%20Integration/VT%20supports%20upgrade%20-%20installation%20tooling/Tooling.docx?x-access-token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2MDUwMTY5MTYsImFjY291bnRfaWQiOiJ2bmFzY2ltZSIsImdyb3VwcyI6W10sImRpc3BsYXlfbmFtZSI6IlZhc2NvIE1pZ3VlbCBOYXNjaW1lbnRvIE1hcnRpbnMgKHZuYXNjaW1lKSJ9.gDlIP5Ewym1dUkxYRzH4MDOkpaiZhkfhsKhUqdJCqJI",
			//   "code": 500,
			//   "size": 0,
			//   "referer": "",
			//   "agent": "Node.js/6.13",
			//   "file": "/var/log/ocproxy/ocproxy_http.log",
			//   "tag": "td.var.log.ocproxy.ocproxy_http.log",
			//   "hostname": "cbox-lbweb-03",
			//   "hostgroup": "box/lbweb",
			//   "shostgroup": "box.lbweb",
			//   "environment": "production",
			//   "time": "2020-11-09T15:07:28.000000000Z"
			// }
			// path is /index.php/apps/onlyoffice/storage/track/myfile.docx?x-access-token=eyJ
			u, err := url.Parse(jl.Path)
			if err != nil {
				er(err)
			}

			p := u.Path
			token := u.Query().Get("x-access-token")
			// we need to strip the path to get the relative part
			p = strings.TrimPrefix(p, "/index.php/apps/onlyoffice/storage/track/")
			eosInfo, err := demangle(p, token)
			eosPaths[eosInfo] = true

		}

		fmt.Printf("invalid records (parsing errors): %d\n", len(invalid))
		fmt.Println(invalid)

		fmt.Println("invalid eos records (error when getting info from eos, not found for example): ", len(invalidEos))
		for _, r := range invalidEos {
			fmt.Println(r)
		}

		fmt.Println("invalid records (lying on eos recycle or version folders, meaning file has been updated): ", len(invalidSkipPath))
		for _, r := range invalidSkipPath {
			fmt.Println(r)
		}
		fmt.Printf("invalid skip records (file is newer than in OO): %d\n", len(invalidSkip))

		// forgotten paths
		fmt.Println("forgotten paths: ", len(forgottenPaths))
		for r := range forgottenPaths {
			fmt.Println(r)
		}

		// paths that failed to be written, potencial lost files
		// from the logs
		fmt.Println("failed writes (POST 500 to cbox): ", len(eosPaths))
		for path := range eosPaths {
			fmt.Println(path)
		}

		// analyze records and compare them with eos paths
		// if we have a match in records array of an eos path found in the logs that means
		// the file was written to OO but never to CERNBox AND the file was not overwritten
		// with a new version, meaning that if we don't tranfer these files they will be lost.
		// paths that are in the cache should be on the 500 error list, writes only
		// we check that if the path in the record is part of the 500 error list
		// we have a candidate for lost file

		lost := []*record{}
		for _, r := range records {
			for path := range eosPaths {
				fmt.Println("diff", r.fi.File, path)
				if r.fi.File == path {
					lost = append(lost, r)
				}

			}
		}

		fmt.Println("lost files, need retransfer: ", len(lost))
		for _, r := range lost {
			fmt.Println("fuck, recover it: ", r.fi.File)
		}

	},
}

func epochStringToTime(s string) (time.Time, error) {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Now(), err
	}
	return time.Unix(i, 0), nil
}

type record struct {
	line         string
	key          string
	epochOO      time.Time
	currentEpoch time.Time
	fi           *eosclient.FileInfo
}
type jsonLine struct {
	Host        string      `json:"host"`
	User        interface{} `json:"user"`
	Method      string      `json:"method"`
	Path        string      `json:"path"`
	Code        int         `json:"code"`
	Size        int         `json:"size"`
	Referer     string      `json:"referer"`
	Agent       string      `json:"agent"`
	File        string      `json:"file"`
	Tag         string      `json:"tag"`
	Hostname    string      `json:"hostname"`
	Hostgroup   string      `json:"hostgroup"`
	Shostgroup  string      `json:"shostgroup"`
	Environment string      `json:"environment"`
	Time        time.Time   `json:"time"`
}

func demangle(p, t string) (string, error) {
	var eospath string
	if strings.HasPrefix(p, "__myprojects") {
		eospath = strings.Split(p, "__myprojects/")[1]
		eospath = fmt.Sprintf("/eos/project/%s/%s", string(eospath[0]), eospath)
	} else if strings.HasPrefix(p, "__myshares/") {
		eospath = strings.TrimPrefix(p, "__myshares/")
		tokens := strings.Split(eospath, "/")
		shareString := tokens[0] // AG2019 20(id:210627)
		shareID := strings.TrimSuffix(strings.Split(shareString, "id:")[1], ")")
		shares, err := getSharesByID(shareID)
		if err != nil {
			return "", err
		}

		if len(shares) == 0 {
			return "", errors.New("share " + shareID + " does not exist")
		}

		share := shares[0]
		filename := path.Join(tokens[1:]...)
		filename = path.Join(share.GetPath(), filename)
		return filename, nil
	} else {
		// path is relative to user
		// we unpack the access toke to get username
		// get second part of JWT token and base64 decode
		payload := strings.Split(t, ".")[1]

		jsonString, err := base64.RawURLEncoding.DecodeString(payload)
		if err != nil {
			return "", err
		}
		type jsonPayload struct {
			Username string `json:"account_id"`
			Token    string `json:"token"`
		}

		jp := &jsonPayload{}
		if err := json.Unmarshal(jsonString, jp); err != nil {
			return "", err
		}

		username := jp.Username
		if username == "" {
			// means is a public link, so we get the path and resolve it
			// so we get token and resolve to eos path
			pls, err := getSharesByToken(jp.Token)
			if err != nil {
				return "", err
			}
			pl := pls[0]
			return path.Join(pl.GetPath(), p), nil

		}

		filename := fmt.Sprintf("/eos/user/%s/%s", string(username[0]), username)
		filename = path.Join(filename, p)
		return filename, nil
	}

	return eospath, nil
}
