package sqlite

import (
	"flag"

	"github.com/spf13/viper"

	"github.com/jaegertracing/jaeger/pkg/sqlite/config"
)

const db_path = "sqlite.db-path"
const cache_size = "sqlite.cache-size"

// Options stores the configuration entries for this storage
type Options struct {
	Configuration config.Configuration
}

// AddFlags from this storage to the CLI
func (opt *Options) AddFlags(flagSet *flag.FlagSet) {
	flagSet.String(db_path, opt.Configuration.DBPath, "SQLite database storage path")
	flagSet.Int(cache_size, opt.Configuration.CacheSize, "LRU cache for SQLite database storage")
}

// InitFromViper initializes the options struct with values from Viper
func (opt *Options) InitFromViper(v *viper.Viper) {
	opt.Configuration.DBPath = v.GetString(db_path)
	opt.Configuration.CacheSize = v.GetInt(cache_size)
}
