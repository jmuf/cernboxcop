package cmd

import (
	"context"
	"fmt"
	"github.com/cs3org/reva/pkg/eosclient"
	"github.com/spf13/cobra"
	"strings"
	"time"
)

func init() {
	rootCmd.AddCommand()
	rootCmd.AddCommand(eosCmd)
	eosCmd.AddCommand(eosQuotaCmd)
}

var eosCmd = &cobra.Command{
	Use:   "eos",
	Short: "Eos Info",
}

var eosQuotaCmd = &cobra.Command{
	Use:   "quota <username>",
	Short: "Returns quota for user",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			exit(cmd)
		}

		username := strings.TrimSpace(args[0])
		quota := getEosQuotaForUser(username)
		fmt.Printf("Available: %d\nUsed: %d\n", quota.AvailableBytes, quota.UsedBytes)

	},
}

func getEosQuotaForUser(username string) *eosclient.QuotaInfo {
	ctx, _ := context.WithTimeout(getCtx(), time.Second*60)
	eos := getEOSForUser(username)
	quota, err := eos.GetQuota(ctx, username, "/eos/user/")
	if err != nil {
		er(err)
	}
	return quota
}

func getEosQuota(mgm, username string) *eosclient.QuotaInfo {
	ctx, _ := context.WithTimeout(getCtx(), time.Second*60)
	eos := getEOS(mgm)
	quota, err := eos.GetQuota(ctx, username, "/eos/user/")
	if err != nil {
		er(err)
	}
	return quota
}

func getEOSQuota(mgm string, uid uint64) *eosclient.QuotaInfo {
	ctx, _ := context.WithTimeout(getCtx(), time.Second*10)
	eos := getEOS(mgm)
	username := fmt.Sprintf("%d", uid)
	quota, err := eos.GetQuota(ctx, username, "/eos/user/")
	if err != nil {
		er(err)
	}
	return quota
}

func getEOSProjectQuota(mgm string, uid uint64) *eosclient.QuotaInfo {
	ctx, _ := context.WithTimeout(getCtx(), time.Second*60)
	eos := getEOS(mgm)
	username := fmt.Sprintf("%d", uid)
	quota, err := eos.GetQuota(ctx, username, "/eos/project/")
	if err != nil {
		er(err)
	}
	return quota
}
