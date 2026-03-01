package storage_s3

import (
	"github.com/infrago/infra"
	"github.com/infrago/storage"
)

func Driver() storage.Driver {
	return &s3Driver{}
}

func init() {
	infra.Register("s3", Driver())
}
