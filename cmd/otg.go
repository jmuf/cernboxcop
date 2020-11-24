package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

func init() {
	rootCmd.AddCommand(otgCmd)

	otgCmd.AddCommand(otgCreateCmd)
	otgCmd.AddCommand(otgDeleteCmd)

	otgCreateCmd.Flags().StringP("otg", "o", "", "OTG number as copy/pasted from the CERN SSB portal")

}

var otgCmd = &cobra.Command{
	Use:   "otg",
	Short: "OTG declaration",
}

var otgCreateCmd = &cobra.Command{
	Use:   "create <htmlmessage>",
	Short: "Creates an OTG with the given htlm message",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			exit(cmd)
		}

		otgNumber, _ := cmd.Flags().GetString("otg")
		if otgNumber == "" {
			exit(cmd)
		}

		template := "<b><a href='https://cern.service-now.com/service-portal/?id=outage&n=%s' target='_blank'>%s: %s</a></b>"
		message := fmt.Sprintf(template, otgNumber, otgNumber, args[0])
		deleteOTG() // clean all otgs
		addOTG(message)
	},
}

var otgDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete existing OTG",
	Run: func(cmd *cobra.Command, args []string) {
		deleteOTG()
	},
}

func addOTG(msg string) {
	db := getDB()
	stmtString := "insert into cbox_otg(message) values (?)"
	stmt, err := db.Prepare(stmtString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error updating inserting otg with msg=%s\n", msg)
		er(err)
	}

	_, err = stmt.Exec(msg)
	if err != nil {
		er(err)
	}
}

func deleteOTG() {
	db := getDB()
	stmtString := "delete from cbox_otg"
	stmt, err := db.Prepare(stmtString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error deleting otg\n")
		er(err)
	}

	_, err = stmt.Exec()
	if err != nil {
		er(err)
	}
}
