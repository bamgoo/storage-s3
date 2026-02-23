module github.com/bamgoo/storage-s3

go 1.25.3

require (
	github.com/aws/aws-sdk-go-v2 v1.32.4
	github.com/aws/aws-sdk-go-v2/config v1.28.4
	github.com/aws/aws-sdk-go-v2/credentials v1.17.45
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.17.38
	github.com/aws/aws-sdk-go-v2/service/s3 v1.67.0
	github.com/bamgoo/bamgoo v0.0.0
	github.com/bamgoo/storage v0.0.0
)

replace github.com/bamgoo/bamgoo => ../bamgoo
replace github.com/bamgoo/storage => ../storage
