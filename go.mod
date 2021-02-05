module github.com/cernbox/cernboxcop

go 1.13

require (
	github.com/cs3org/reva v1.0.1-0.20200804150337-95e8788a1093
	github.com/dustin/go-humanize v1.0.0
	github.com/go-redis/redis v6.15.7+incompatible
	github.com/go-sql-driver/mysql v1.5.0
	github.com/leekchan/accounting v0.0.0-20191218023648-17a4ce5f94d4
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mitchellh/mapstructure v1.2.2
	github.com/olekukonko/tablewriter v0.0.4
	github.com/onsi/ginkgo v1.12.0 // indirect
	github.com/onsi/gomega v1.9.0 // indirect
	github.com/rs/zerolog v1.18.0
	github.com/spf13/cobra v0.0.6
	github.com/spf13/viper v1.6.2
	github.com/tj/go-spin v1.1.0
	go.etcd.io/bbolt v1.3.2
	gopkg.in/ldap.v3 v3.1.0
)

replace github.com/cs3org/reva => github.com/labkode/reva v0.0.0-20200421155327-0546020c3ee9
