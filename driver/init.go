package driver

import (
	"context"
	"fmt"

	"github.com/sinmetal/alminium_spanner/config"
	"github.com/sinmetal/alminium_spanner/driver/driver"
	"github.com/sinmetal/alminium_spanner/driver/spanner"
	"github.com/sinmetal/alminium_spanner/driver/mysql"
)

// Init provide common init client func
func Init(ctx context.Context, cfg *config.Config) (driver.Driver, error) {
	switch cfg.Mode {
	case "spanner":
		return spanner.Init(ctx, cfg)
	case "mysql":
		return mysql.Init(ctx, cfg)
	default:
		panic(fmt.Sprintf("unhandled target %s", cfg.Mode))
	}
}
