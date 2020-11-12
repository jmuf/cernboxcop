package cmd

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/cs3org/reva/pkg/appctx"
	"github.com/cs3org/reva/pkg/eosclient"
	"github.com/go-redis/redis"
	_ "github.com/go-sql-driver/mysql"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/olekukonko/tablewriter"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/ldap.v3"
)

var (
	// Used for flags.
	cfgFile string

	log *zerolog.Logger

	rootCmd = &cobra.Command{
		Use:   "cernboxcop",
		Short: "This tool will make your life easier",
	}
)

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	loadConfig()
}
func loadConfig() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "/etc/cernboxcop/cernboxcop.yaml", "config file")
}

func er(msg interface{}) {
	fmt.Println("Error:", msg)
	os.Exit(1)
}

func exit(cmd *cobra.Command) {
	cmd.Usage()
	os.Exit(1)
}

func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			er(err)
		}

		// Search config in home directory with name ".cobra" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".cernboxcop")
	}

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		er(err)
	}

	logfile := viper.GetString("logfile")
	log = newLogger(logfile)
}

func getDB() *sql.DB {
	username := viper.GetString("db_username")
	password := viper.GetString("db_password")
	hostname := viper.GetString("db_hostname")
	port := viper.GetInt("db_port")
	dbname := viper.GetString("db_name")

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", username, password, hostname, port, dbname))
	if err != nil {
		er(err)
	}
	return db
}

func getRedis() *redis.Client {
	address := viper.GetString("redis_address")
	password := viper.GetString("redis_password")

	redisOpts := &redis.Options{
		Network:  "tcp",
		Addr:     address,
		Password: password,
	}

	return redis.NewClient(redisOpts)
}

func getLDAP() *ldap.Conn {
	host := viper.GetString("ldap_host")
	port := viper.GetInt("ldap_port")
	l, err := ldap.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		er(err)
	}
	return l
}

func getEOS(mgm string) *eosclient.Client {
	eosClientOpts := &eosclient.Options{
		URL: mgm,
	}

	eosClient := eosclient.New(eosClientOpts)
	return eosClient
}

func getEOSForUser(username string) *eosclient.Client {
	letter := string(username[0])
	mgm := fmt.Sprintf("root://eoshome-%s.cern.ch", letter)
	return getEOS(mgm)
}

func getProbeUser() (string, string) {
	return viper.GetString("probe_username"), viper.GetString("probe_password")
}

func getProbeEOSInstances() []string {
	return viper.GetStringSlice("probe_eos_instances")
}

func saveWith(file string, data []byte) {
	fd, err := os.Create(file)
	if err != nil {
		er(err)
	}
	fd.Write(data)
}

func save(cols []string, rows [][]string, file string) {
	// make sure all parent directories exist
	os.MkdirAll(path.Dir(file), 0755)
	fd, err := os.Create(file)
	if err != nil {
		er(err)
	}
	table := tablewriter.NewWriter(fd)
	table.SetHeader(cols)
	table.SetAutoWrapText(false)
	table.SetAutoFormatHeaders(true)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetCenterSeparator("")
	table.SetColumnSeparator("")
	table.SetRowSeparator("")
	table.SetHeaderLine(false)
	table.SetBorder(false)
	table.SetTablePadding("\t") // pad with tabs
	table.SetNoWhiteSpace(true)
	table.AppendBulk(rows) // Add Bulk Data
	table.Render()
}
func pretty(cols []string, rows [][]string) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(cols)
	table.SetAutoWrapText(false)
	table.SetAutoFormatHeaders(true)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetCenterSeparator("")
	table.SetColumnSeparator("")
	table.SetRowSeparator("")
	table.SetHeaderLine(false)
	table.SetBorder(false)
	table.SetTablePadding("\t") // pad with tabs
	table.SetNoWhiteSpace(true)
	table.AppendBulk(rows) // Add Bulk Data
	table.Render()
}

func newLogger(logfile string) *zerolog.Logger {
	w := getWriter(logfile)
	zl := zerolog.New(w).With().Timestamp().Caller().Logger()
	return &zl
}

func getWriter(out string) io.Writer {
	if out == "stderr" || out == "" {
		return os.Stderr
	}

	if out == "stdout" {
		return os.Stdout
	}

	fd, err := os.Create(out)
	if err != nil {
		er(err)
	}

	return fd
}

func getCtx() context.Context {
	ctx := context.Background()
	ctx = appctx.WithLogger(ctx, log)
	return ctx
}

// From: https://gist.github.com/r0l1/3dcbb0c8f6cfe9c66ab8008f55f8f28b
// askForConfirmation asks the user for confirmation. A user must type in "yes" or "no" and
// then press enter. It has fuzzy matching, so "y", "Y", "yes", "YES", and "Yes" all count as
// confirmations. If the input is not recognized, it will ask again. The function does not return
// until it gets a valid response from the user.
func askForConfirmation(s string) bool {
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Printf("%s [y/n]: ", s)

		response, err := reader.ReadString('\n')
		if err != nil {
			er(err)
		}

		response = strings.ToLower(strings.TrimSpace(response))

		if response == "y" || response == "yes" {
			return true
		} else if response == "n" || response == "no" {
			return false
		}
	}
}
