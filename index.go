package storage_s3

import (
	"github.com/bamgoo/bamgoo"
	"github.com/bamgoo/storage"
)

func Driver() storage.Driver {
	return &s3Driver{}
}

func init() {
	bamgoo.Register("s3", Driver())
}
