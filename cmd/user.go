package cmd

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/spf13/cobra"
	"gopkg.in/ldap.v3"
)

func init() {
	rootCmd.AddCommand(userCmd)
	userCmd.AddCommand(userCheckCmd)
	userCmd.AddCommand(userGroupsCmd)
	userCmd.AddCommand(userListInactiveCmd)
}

var userCmd = &cobra.Command{
	Use:   "user",
	Short: "User Info",
}

var userCheckCmd = &cobra.Command{
	Use:   "check <username>",
	Short: "Checks the user for a healthy state",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			exit(cmd)
		}

		lc := getLDAP()
		username := strings.TrimSpace(args[0])
		info := getUserFull(lc, username)

		infos := []*userInfo{info}
		if info.AccountOwner != nil && info.AccountOwner.Account != info.Account {
			infos = append(infos, info.AccountOwner)
		}
		prettyUser(infos...)

	},
}

var userGroupsCmd = &cobra.Command{
	Use:   "groups <username>",
	Short: "Retrieves the groups the the user is member of",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			exit(cmd)
		}

		username := strings.TrimSpace(args[0])
		groups := getUserGroups(username)

		for _, g := range groups {
			fmt.Println(g)
		}
	},
}

var userListInactiveCmd = &cobra.Command{
	Use:   "list-inactive <days>",
	Short: "Lists the user accounts which have been inactive for more than a given number of days",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			exit(cmd)
		}

		days, err := strconv.Atoi(strings.TrimSpace(args[0]))
		if err != nil {
			log.Err(err).Msgf("value for days is not an integer: %s", args[0])
		}

		apiEP, token, err := getGrappaURLAndToken()
		if err != nil {
			log.Err(err).Msg("error getting token")
		}

		info, err := getInactiveUsers(apiEP, token, days)
		if err != nil {
			log.Err(err).Msg("error getting token")
		}

		prettyUser(info...)

	},
}

func prettyUser(userInfos ...*userInfo) {
	cols := []string{"Account", "Type", "Name", "Department", "Group", "Section", "Mail", "Phone"}
	rows := make([][]string, 0, len(userInfos))
	for _, ui := range userInfos {
		row := []string{ui.Account, ui.AccountType, ui.Name, ui.Department, ui.Group, ui.Section, ui.Mail, ui.Phone}
		rows = append(rows, row)
	}
	pretty(cols, rows)
}

func prettyInactiveUsers(userInfos ...*userInfo) {
	cols := []string{"Account", "Type", "Name", "Department", "Group", "Section", "Mail", "Phone", "Inactive since"}
	rows := make([][]string, 0, len(userInfos))
	for _, ui := range userInfos {
		row := []string{ui.Account, ui.AccountType, ui.Name, ui.Department, ui.Group, ui.Section, ui.Mail, ui.Phone, ui.BlockingTime.String()}
		rows = append(rows, row)
	}
	pretty(cols, rows)
}

func getHomePath(username string) string {
	letter := string(username[0])
	return fmt.Sprintf("/eos/user/%s/%s", letter, username)
}

func isMigrated(username string) bool {
	rdb := getRedis()
	key := getHomePath(username)
	val, err := rdb.Get(key).Result()
	if err != nil {
		if err == redis.Nil {
			return false
		}
		er(err)
	}

	if val == "migrated" {
		return true
	}

	if val == "non-migrated" {
		return false
	}

	err = errors.New("wrong redis key for user:" + username)
	er(err)
	return false
}

func getUser(l *ldap.Conn, uid string) *userInfo {

	// Search for the given username
	searchTerm := fmt.Sprintf("(&(objectClass=user)(samaccountname=%s))", uid)
	searchRequest := ldap.NewSearchRequest(
		"OU=Users,OU=Organic Units,DC=cern,DC=ch",
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		searchTerm,
		[]string{},
		//[]string{"dn", "cn", "displayName", "mail", "cernAccountType"},
		nil,
	)

	sr, err := l.Search(searchRequest)
	if err != nil {
		return newUserInfo()
	}

	if len(sr.Entries) == 0 {
		return newUserInfo()
	}

	entry := sr.Entries[0]
	ui := newUserInfo()
	for _, attr := range entry.Attributes {
		if attr.Name == "cn" {
			ui.Account = attr.Values[0]
		}
		if attr.Name == "displayName" {
			ui.Name = attr.Values[0]
		}
		if attr.Name == "cernAccountType" {
			ui.AccountType = attr.Values[0]
		}
		if attr.Name == "mail" {
			ui.Mail = attr.Values[0]
		}
		if attr.Name == "telephoneNumber" {
			ui.Phone = attr.Values[0]
		}
		if attr.Name == "division" {
			ui.Department = attr.Values[0]
		}
		if attr.Name == "cernGroup" {
			ui.Group = attr.Values[0]
		}
		if attr.Name == "cernSection" {
			ui.Section = attr.Values[0]
		}
		if attr.Name == "cernAccountOwner" {
			ui.AccountOwnerDN = attr.Values[0]
		}
		if attr.Name == "uidNumber" {
			ui.UID = attr.Values[0]
		}
		if attr.Name == "gidNumber" {
			ui.GID = attr.Values[0]
		}
	}

	return ui
}

func getUserFull(lc *ldap.Conn, uid string) *userInfo {
	ui := getUser(lc, uid)

	// if account is service we get the owner details
	if ui.AccountType == "Service" || ui.AccountType == "Secondary" {
		cn := extractCN(ui.AccountOwnerDN)
		owner := getUser(lc, cn)
		ui.AccountOwner = owner
	} else if ui.AccountType == "Primary" {
		ui.AccountOwner = ui
	}

	return ui
}

// CN=gonzalhu,OU=Users,OU=Organic Units,DC=cern,DC=ch
// returns gonzalhu
func extractCN(dn string) string {
	if dn == "" {
		return ""
	}
	tokens := strings.Split(dn, ",")
	tokens = strings.Split(tokens[0], "=")
	return tokens[1]
}

func getUserGroups(uid string) []string {
	l := getLDAP()
	defer l.Close()

	searchRequest := ldap.NewSearchRequest(
		fmt.Sprintf("CN=%s,OU=Users,OU=Organic Units,DC=cern,DC=ch", uid),
		ldap.ScopeBaseObject, ldap.NeverDerefAliases, 0, 0, false,
		"(objectClass=User)",
		[]string{"tokenGroups"},
		nil,
	)

	sr, err := l.SearchWithPaging(searchRequest, 1000000)
	if err != nil {
		er(err)
	}

	var sids []string
	for _, entry := range sr.Entries {
		for _, attr := range entry.Attributes {
			if attr.Name == "tokenGroups" {
				for _, binarySID := range attr.ByteValues {
					numSubIDs, _ := strconv.ParseUint(fmt.Sprintf("%d", binarySID[1]), 16, 64)
					auth, _ := strconv.ParseUint(fmt.Sprintf("%x", binarySID[2:8]), 16, 64)

					sidObject := fmt.Sprintf("S-%x-%d", binarySID[0], auth)
					//authorities := []uint32{}
					for i := uint64(0); i < numSubIDs; i++ {
						part := binarySID[8+4*i : 12+4*i]
						a := binary.LittleEndian.Uint32(part)
						sidObject += fmt.Sprintf("-%d", a)
						//authorities = append(authorities, a)
					}

					//fmt.Printf("SID_RAW=%X SRL=%x NUM_SUB_ID=%d AUTH=%d AUTHORITIES=%+v SID=%s\n", binarySID, binarySID[0], numSubIDs, auth, authorities, sidObject)
					sids = append(sids, sidObject)
				}
			}
		}
	}

	groupsFilter := "(&(objectClass=Group)(|%s))"
	var query string
	for _, sid := range sids {
		query += fmt.Sprintf("(objectSID=%s)", sid)
	}
	groupsFilter = fmt.Sprintf(groupsFilter, query)

	searchRequest = ldap.NewSearchRequest(
		"OU=e-groups,OU=Workgroups,DC=cern,DC=ch",
		ldap.ScopeSingleLevel, ldap.NeverDerefAliases, 0, 0, false,
		groupsFilter,
		[]string{"cn"},
		nil,
	)

	sr, err = l.SearchWithPaging(searchRequest, 1000000)
	if err != nil {
		er(err)
	}

	var gids []string
	for _, entry := range sr.Entries {
		for _, attr := range entry.Attributes {
			if attr.Name == "cn" {
				for _, cn := range attr.Values {
					gids = append(gids, cn)
				}
			}
		}
	}
	return gids
}

func getInactiveUsers(apiEP, token string, days int) ([]*userInfo, error) {
	url := apiEP + "/Identity?filter=activeUser:false&field=upn&field=primaryAccountEmail&field=displayName&field=type&field=cernGroup&field=cernDepartment&field=cernSection&field=uid&field=gid"
	users := []*userInfo{}
	for {
		u, nextPage, err := getInactiveUsersByPage(url, token)
		if err != nil {
			return nil, err
		}
		users = append(users, u...)
		url = apiEP + nextPage
		if nextPage == "" {
			break
		}
	}
	return filterUsersByDate(users, days)
}

func getInactiveUsersByPage(url string, token string) ([]*userInfo, string, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	res, err := client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, "", err
	}

	var result map[string]interface{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, "", err
	}
	userData, ok := result["data"].([]interface{})
	if !ok {
		return nil, "", errors.New("rest: error in type assertion")
	}

	users := []*userInfo{}
	for _, u := range userData {
		info, ok := u.(map[string]interface{})
		if !ok {
			return nil, "", errors.New("rest: error in type assertion")
		}
		users = append(users, &userInfo{
			Account:     info["upn"].(string),
			Name:        info["displayName"].(string),
			Mail:        info["primaryAccountEmail"].(string),
			UID:         info["uid"].(string),
			GID:         info["gid"].(string),
			AccountType: info["type"].(string),
			Department:  info["cernDepartment"].(string),
			Group:       info["cernGroup"].(string),
			Section:     info["cernSection"].(string),
		})
	}

	var nextPage string
	if pagination, ok := result["pagination"].(map[string]interface{}); ok {
		if links, ok := pagination["links"].(map[string]string); ok {
			nextPage = links["next"]
		}
	}

	return users, nextPage, nil

}

func filterUsersByDate(users []*userInfo, days int) ([]*userInfo, error) {
	// TODO: Add filters once bug in grappa is fixed
	return users, nil
}

func newUserInfo() *userInfo {
	return &userInfo{
		AccountOwner: &userInfo{},
	}
}

type userInfo struct {
	UID, GID       string
	Account        string
	Name           string
	Mail           string
	AccountType    string
	Department     string
	Group          string
	Section        string
	AccountOwner   *userInfo
	AccountOwnerDN string
	Phone          string
	BlockingTime   time.Time
}

func (ui *userInfo) accountTypeHuman() string {
	if ui.AccountType != "" {
		return ui.AccountType + "-Account"
	}
	return ""
}
