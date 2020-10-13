package cmd

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-redis/redis"
	"github.com/spf13/cobra"
	"gopkg.in/ldap.v3"
)

func init() {
	rootCmd.AddCommand(userCmd)
	userCmd.AddCommand(userCheckCmd)
	userCmd.AddCommand(userGroupsCmd)
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

var prettyUser = func(userInfos ...*userInfo) {
	cols := []string{"Account", "Type", "Name", "Department", "Group", "Section", "Mail", "Phone"}
	rows := make([][]string, 0, len(userInfos))
	for _, ui := range userInfos {
		row := []string{ui.Account, ui.AccountType, ui.Name, ui.Department, ui.Group, ui.Section, ui.Mail, ui.Phone}
		rows = append(rows, row)
	}
	pretty(cols, rows)
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
}

func (ui *userInfo) accountTypeHuman() string {
	if ui.AccountType != "" {
		return ui.AccountType + "-Account"
	}
	return ""
}
