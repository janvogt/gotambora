package coding

import (
	"database/sql"
	"github.com/mattes/migrate/migrate"
)

type ConfigType struct {
	MigrationsPath string
	Db             *sql.DB
	DbUrl          string
}

var config = ConfigType{}

const currentMigration uint64 = 0

// Get the singleton configuration object
func Config() *ConfigType {
	return &config
}

func Migrate() (err []error) {
	if v, e := migrate.Version(Config().DbUrl, Config().MigrationsPath); v < currentMigration {
		err, _ = migrate.UpSync(Config().DbUrl, Config().MigrationsPath)
	} else if e != nil {
		err = append(err, e)
	}
	return err
}
