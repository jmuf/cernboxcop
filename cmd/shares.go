package cmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"strconv"
	"strings"
)

func init() {
	rootCmd.AddCommand(shareCmd)

	shareCmd.AddCommand(shareListCmd)
	shareCmd.AddCommand(shareTransferCmd)

	shareListCmd.Flags().StringP("owner", "o", "", "filter by owner account")
	shareListCmd.Flags().StringP("id", "i", "", "filter by share id")
	shareListCmd.Flags().StringP("token", "t", "", "filter by public link token")
	shareListCmd.Flags().StringP("share-with", "s", "", "filter by share with (username or egroup)")
	shareListCmd.Flags().StringP("path", "p", "", "filter by eos path")
	shareListCmd.Flags().BoolP("all", "a", false, "shows all shares")
	shareListCmd.Flags().BoolP("printpath", "", false, "print EOS path, it can be expensive depending on number of shares")

	shareTransferCmd.Flags().BoolP("yes", "y", false, "confirms transfership of ownership without confirmation")
}

var shareCmd = &cobra.Command{
	Use:   "sharing",
	Short: "Sharing info",
}

var shareTransferCmd = &cobra.Command{
	Use:   "transfer <share-id> <new-owner> <project>\nExample: cernboxcop sharing transfer 1345 gonzalhu cernbox",
	Short: "Transfer a share to a new owner",
	Run: func(cmd *cobra.Command, args []string) {
		print := func(shares []*dbShare) {
			cols := []string{"ID", "FILEID", "OWNER", "TYPE", "SHARE_WITH", "PERMISSION", "URL", "PATH"}
			rows := [][]string{}
			for _, s := range shares {
				row := []string{fmt.Sprintf("%d", s.ID), s.FileID(), s.UIDOwner, s.HumanType(), s.HumanShareWith(), s.HumanPerm(), s.PublicLink()}
				row = append(row, s.GetPath())
				rows = append(rows, row)
			}
			pretty(cols, rows)
		}

		if len(args) != 3 {
			exit(cmd)
		}

		id := strings.TrimSpace(args[0])
		owner := strings.TrimSpace(args[1])
		projectNameOrPath := strings.TrimSpace(args[2])

		// validate share id
		shares, err := getSharesByID(id)
		if err != nil {
			er(err)
		}

		if len(shares) != 1 {
			fmt.Fprintf(os.Stderr, "Error: share does not exist\n")
			os.Exit(1)
		}

		print(shares)
		share := shares[0]

		// check share points to a project
		if !strings.Contains(share.Prefix, "project") {
			fmt.Fprintf(os.Stderr, "Error: the share does not point to file/folder inside an EOS project. Only shared on projects can be transfered\n")
			os.Exit(1)
		}

		// check project exists
		projectInfo, err := getProject(projectNameOrPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: the project provided %q does not exist\n", projectNameOrPath)
			os.Exit(1)
		}
		// Only admins can create shares on project spaces.
		// Check that the new owner is also in the admin e-group.
		adminGroup := fmt.Sprintf("cernbox-project-%s-admins", projectInfo.name)
		groups := getUserGroups(owner)
		var found bool
		for _, g := range groups {
			if adminGroup == g {
				found = true
				break
			}
		}

		if !found {
			fmt.Fprintf(os.Stderr, "Error: the new owner does not belong to the admin group %q. Only admins can manage shares. Ask the user to join the admin group.\n", adminGroup)
			os.Exit(1)
		}

		yes, _ := cmd.Flags().GetBool("yes")
		if !yes {
			msg := fmt.Sprintf("Are you sure to transfer ownernship from %q to %q?\n", share.UIDOwner, owner)
			c := askForConfirmation(msg)

			if !c {
				fmt.Fprintf(os.Stderr, "Aborted\n")
				os.Exit(1)
			}
		}

		updateShareOwner(share.ID, owner)
	},
}

var shareListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all the shares",
	Run: func(cmd *cobra.Command, args []string) {
		print := func(shares []*dbShare) {
			printpath, _ := cmd.Flags().GetBool("printpath")
			cols := []string{"ID", "FILEID", "OWNER", "TYPE", "SHARE_WITH", "PERMISSION", "URL", "PATH"}
			rows := [][]string{}
			for _, s := range shares {
				row := []string{fmt.Sprintf("%d", s.ID), s.FileID(), s.UIDOwner, s.HumanType(), s.HumanShareWith(), s.HumanPerm(), s.PublicLink()}
				if printpath {
					row = append(row, s.GetPath())
				}
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

func (s *dbShare) GetPath() string {
	inode, err := strconv.ParseUint(s.ItemSource, 10, 64)
	if err != nil {
		er(err)
	}

	mgm := fmt.Sprintf("root://%s.cern.ch", s.Prefix)
	mgm = strings.ReplaceAll(mgm, "new", "eos")
	client := getEOS(mgm)
	ctx := context.Background()
	fi, err := client.GetFileInfoByInode(ctx, "root", inode)
	if err != nil {
		return "-"
	}
	return fi.File
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

func updateShareOwner(shareId int, newOwner string) {
	// check that args are valid.
	if shareId == 0 {
		fmt.Fprintf(os.Stderr, "Error: shareId is 0\n")
		os.Exit(1)
	}

	if newOwner == "" {
		fmt.Fprintf(os.Stderr, "Error: new owner is empty\n")
		os.Exit(1)
	}

	db := getDB()
	stmtString := "update oc_share set uid_owner=? where id=?"
	stmt, err := db.Prepare(stmtString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error updating share owner for id=%s with new owner=%s\n", shareId, newOwner)
		er(err)
	}

	_, err = stmt.Exec(newOwner, shareId)
	if err != nil {
		er(err)
	}
}
