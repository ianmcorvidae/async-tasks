package main

import (
	_ "expvar"
	"flag"
	// "net/http"

	"github.com/cyverse-de/configurate"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var log = logrus.WithFields(logrus.Fields{
        "service": "async-tasks",
        "art-id":  "async-tasks",
        "group":   "org.cyverse",
})

func init() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
}

func main() {
	var (
		cfgPath = flag.String("config", "/etc/iplant/de/async-tasks.yml", "The path to the config file")
		// port    = flag.String("port", "60000", "The port number to listen on")
		err error
		cfg *viper.Viper
	)

	flag.Parse()

	if *cfgPath == "" {
		log.Fatal("--config must not be the empty string")
	}

	if cfg, err = configurate.Init(*cfgPath); err != nil {
		log.Fatal(err.Error())
	}

	dburi := cfg.GetString("db.uri")

	db, err := SetupDB(dburi)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.db.Close()

	row := db.db.QueryRow("SELECT COUNT(*) FROM async_tasks");
	var res struct{count int}
	row.Scan(&res)
	log.Infof("There are %d async tasks in the database", res.count)
}
