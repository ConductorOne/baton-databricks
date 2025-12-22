package main

import (
	cfg "github.com/conductorone/baton-databricks/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/config"
)

func main() {
	config.Generate("databricks", cfg.Config)
}
