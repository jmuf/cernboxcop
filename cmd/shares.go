package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"strings"
)

func init() {
	rootCmd.AddCommand(shareCmd)

	shareCmd.AddCommand(shareListCmd)
	shareListCmd.Flags().StringP("owner", "o", "", "filter by owner account")
	shareListCmd.Flags().StringP("id", "i", "", "filter by share id")
	shareListCmd.Flags().StringP("token", "t", "", "filter by public link token")
	shareListCmd.Flags().StringP("share-with", "s", "", "filter by share with (username or egroup)")
	shareListCmd.Flags().StringP("path", "p", "", "filter by eos path")
	shareListCmd.Flags().BoolP("all", "a", false, "shows all shares")
}

var shareCmd = &cobra.Command{
	Use:   "sharing",
	Short: "Sharing info",
}

var shareListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all the shares",
	Run: func(cmd *cobra.Command, args []string) {
		print := func(shares []*dbShare) {
			cols := []string{"ID", "FILEID", "OWNER", "TYPE", "SHARE_WITH", "PERMISSION", "URL"}
			rows := [][]string{}
			for _, s := range shares {
				row := []string{fmt.Sprintf("%d", s.ID), s.FileID(), s.UIDOwner, s.HumanType(), s.HumanShareWith(), s.HumanPerm(), s.PublicLink()}
				rows = append(rows, row)
			}
			pretty(cols, rows)
			os.Exit(0)
		}

		owner, _ := cmd.Flags().GetString("owner")
		owner = strings.TrimSpace(owner)
		if owner != "" {
			shares, err := getSharesByOwner(owner)
			if err != nil {
				er(err)
			}
			print(shares)
		}

		id, _ := cmd.Flags().GetString("id")
		id = strings.TrimSpace(id)
		if id != "" {
			shares, err := getSharesByID(id)
			if err != nil {
				er(err)
			}
			print(shares)
		}

		with, _ := cmd.Flags().GetString("share-with")
		with = strings.TrimSpace(with)
		if with != "" {
			shares, err := getSharesByWith(with)
			if err != nil {
				er(err)
			}
			print(shares)
		}

		token, _ := cmd.Flags().GetString("token")
		token = strings.TrimSpace(token)
		if token != "" {
			shares, err := getSharesByToken(token)
			if err != nil {
				er(err)
			}
			print(shares)
		}

		all, _ := cmd.Flags().GetBool("all")
		if all {
			shares, err := getAllShares()
			if err != nil {
				er(err)
			}
			print(shares)

		}

	},
}

type dbShare struct {
	ID          int
	UIDOwner    string
	Prefix      string
	ItemSource  string
	ShareWith   string
	Permissions int
	ShareType   int
	STime       int
	FileTarget  string
	State       int
	Token       string
}

func (s *dbShare) FileID() string {
	v := fmt.Sprintf("%s:%s", s.Prefix, s.ItemSource)
	// replace internal namespacing for one user friendly.
	// newproject-c => eosproject-c
	v = strings.ReplaceAll(v, "new", "eos")
	return v
}

func (s *dbShare) PublicLink() string {
	if s.ShareType != 3 {
		return "-"
	}
	return fmt.Sprintf("https://cernbox.cern.ch/index.php/s/%s", s.Token)
}

func (s *dbShare) HumanShareWith() string {
	if s.ShareType == 3 {
		return "-"
	}
	return s.ShareWith
}

func (s *dbShare) HumanPerm() string {
	if s.Permissions == 1 {
		return "read-only"
	}
	return "read-write"
}

func (s *dbShare) HumanType() string {
	if s.ShareType == 0 {
		return "user-share"
	}
	if s.ShareType == 1 {
		return "egroup-share"
	}
	if s.ShareType == 3 {
		return "public-link"
	}
	return "unknown"
}

func getSharesByToken(token string) (shares []*dbShare, err error) {
	query := "select id, coalesce(uid_owner, '') as uid_owner,  coalesce(share_with, '') as share_with, coalesce(fileid_prefix, '') as fileid_prefix, coalesce(item_source, '') as item_source, stime, permissions, share_type, coalesce(token, '') as token from oc_share where token=?"
	args := []interface{}{token}

	return getShares(query, args)
}

func getSharesByWith(with string) (shares []*dbShare, err error) {
	query := "select id, coalesce(uid_owner, '') as uid_owner,  coalesce(share_with, '') as share_with, coalesce(fileid_prefix, '') as fileid_prefix, coalesce(item_source, '') as item_source, stime, permissions, share_type, coalesce(token, '') as token from oc_share where share_with=?"
	args := []interface{}{with}

	return getShares(query, args)
}

func getSharesByID(id string) (shares []*dbShare, err error) {
	query := "select id, coalesce(uid_owner, '') as uid_owner,  coalesce(share_with, '') as share_with, coalesce(fileid_prefix, '') as fileid_prefix, coalesce(item_source, '') as item_source, stime, permissions, share_type, coalesce(token, '') as token from oc_share where id=?"
	args := []interface{}{id}

	return getShares(query, args)
}

func getSharesByOwner(owner string) (shares []*dbShare, err error) {
	query := "select id, coalesce(uid_owner, '') as uid_owner,  coalesce(share_with, '') as share_with, coalesce(fileid_prefix, '') as fileid_prefix, coalesce(item_source, '') as item_source, stime, permissions, share_type, coalesce(token, '') as token from oc_share where uid_owner=?"
	args := []interface{}{owner}

	return getShares(query, args)
}

func getAllShares() (shares []*dbShare, err error) {
	query := "select id, coalesce(uid_owner, '') as uid_owner,  coalesce(share_with, '') as share_with, coalesce(fileid_prefix, '') as fileid_prefix, coalesce(item_source, '') as item_source, stime, permissions, share_type, coalesce(token, '') as token from oc_share"
	return getShares(query, nil)
}

func getShares(query string, args []interface{}) (shares []*dbShare, err error) {
	db := getDB()

	var (
		id          int
		uidOwner    string
		shareWith   string
		prefix      string
		itemSource  string
		shareType   int
		stime       int
		permissions int
		token       string
	)

	rows, err := db.Query(query, args...)
	if err != nil {
		er(err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&id, &uidOwner, &shareWith, &prefix, &itemSource, &stime, &permissions, &shareType, &token)
		if err != nil {
			return nil, err
		}
		dbShare := &dbShare{ID: id, UIDOwner: uidOwner, Prefix: prefix, ItemSource: itemSource, ShareWith: shareWith, STime: stime, Permissions: permissions, ShareType: shareType, Token: token}
		shares = append(shares, dbShare)

	}

	err = rows.Err()
	if err != nil {
		er(err)
	}

	return
}
