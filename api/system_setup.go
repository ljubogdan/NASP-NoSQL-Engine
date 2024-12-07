package api

import (
	"NASP-NoSQL-Engine/internal/wal"
)

func SystemSetup() {
	wal.DeleteOldWals()
}