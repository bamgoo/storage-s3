package storage_s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bamgoo/storage"
)

type (
	s3Driver struct{}

	s3Connection struct {
		instance *storage.Instance
		setting  s3Setting
		client   *s3.Client
	}

	s3Setting struct {
		Region       string
		Bucket       string
		AccessKey    string
		SecretKey    string
		SessionToken string
		Endpoint     string
		UsePathStyle bool
	}

	tempStream struct {
		file *os.File
		path string
	}
)

func (d *s3Driver) Connect(instance *storage.Instance) (storage.Connection, error) {
	setting := s3Setting{Region: "us-east-1", Bucket: "default"}
	if v, ok := instance.Setting["region"].(string); ok && v != "" {
		setting.Region = v
	}
	if v, ok := instance.Setting["bucket"].(string); ok && v != "" {
		setting.Bucket = v
	}
	if v, ok := instance.Setting["access"].(string); ok && v != "" {
		setting.AccessKey = v
	}
	if v, ok := instance.Setting["accesskey"].(string); ok && v != "" {
		setting.AccessKey = v
	}
	if v, ok := instance.Setting["access_key"].(string); ok && v != "" {
		setting.AccessKey = v
	}
	if v, ok := instance.Setting["secret"].(string); ok && v != "" {
		setting.SecretKey = v
	}
	if v, ok := instance.Setting["secretkey"].(string); ok && v != "" {
		setting.SecretKey = v
	}
	if v, ok := instance.Setting["secret_key"].(string); ok && v != "" {
		setting.SecretKey = v
	}
	if v, ok := instance.Setting["session_token"].(string); ok && v != "" {
		setting.SessionToken = v
	}
	if v, ok := instance.Setting["endpoint"].(string); ok && v != "" {
		setting.Endpoint = v
	}
	if v, ok := instance.Setting["path_style"].(bool); ok {
		setting.UsePathStyle = v
	}
	if v, ok := instance.Setting["force_path_style"].(bool); ok {
		setting.UsePathStyle = v
	}
	if setting.Bucket == "" {
		setting.Bucket = "default"
	}
	return &s3Connection{instance: instance, setting: setting}, nil
}

func (c *s3Connection) Open() error {
	ctx := context.Background()
	loadOpts := []func(*config.LoadOptions) error{config.WithRegion(c.setting.Region)}
	if c.setting.AccessKey != "" || c.setting.SecretKey != "" || c.setting.SessionToken != "" {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.setting.AccessKey, c.setting.SecretKey, c.setting.SessionToken)))
	}

	cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return err
	}

	clientOpts := []func(*s3.Options){
		func(o *s3.Options) {
			o.UsePathStyle = c.setting.UsePathStyle
		},
	}
	if c.setting.Endpoint != "" {
		ep := c.setting.Endpoint
		if !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
			ep = "https://" + ep
		}
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(ep)
		})
	}

	client := s3.NewFromConfig(cfg, clientOpts...)
	c.client = client

	_, err = c.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(c.setting.Bucket)})
	if err != nil {
		_, err = c.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(c.setting.Bucket)})
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *s3Connection) Health() storage.Health {
	if c.client == nil {
		return storage.Health{Workload: 1}
	}
	return storage.Health{Workload: 0}
}

func (c *s3Connection) Close() error {
	c.client = nil
	return nil
}

func (c *s3Connection) Upload(original string, opt storage.UploadOption) (*storage.File, error) {
	if c.client == nil {
		return nil, errors.New("s3 client not ready")
	}
	st, err := os.Stat(original)
	if err != nil {
		return nil, err
	}
	if st.IsDir() {
		return nil, errors.New("directory upload not supported")
	}
	if opt.Key == "" {
		return nil, errors.New("missing upload key")
	}

	ext := path.Ext(original)
	if len(ext) > 0 {
		ext = ext[1:]
	}
	file := c.instance.NewFile(opt.Prefix, opt.Key, ext, st.Size())
	key := objectPath(file)

	f, err := os.Open(original)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	input := &s3.PutObjectInput{
		Bucket: aws.String(c.setting.Bucket),
		Key:    aws.String(key),
		Body:   f,
	}
	if opt.Mimetype != "" {
		input.ContentType = aws.String(opt.Mimetype)
	}
	if !opt.Expires.IsZero() {
		input.Expires = aws.Time(opt.Expires)
	}
	if len(opt.Metadata) > 0 {
		md := map[string]string{}
		for k, v := range opt.Metadata {
			md[k] = fmt.Sprintf("%v", v)
		}
		input.Metadata = md
	}
	if len(opt.Tags) > 0 {
		keys := make([]string, 0, len(opt.Tags))
		for k := range opt.Tags {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		tags := make([]string, 0, len(keys))
		for _, k := range keys {
			tags = append(tags, fmt.Sprintf("%s=%s", url.QueryEscape(k), url.QueryEscape(fmt.Sprintf("%v", opt.Tags[k]))))
		}
		input.Tagging = aws.String(strings.Join(tags, "&"))
	}

	_, err = c.client.PutObject(context.Background(), input)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (c *s3Connection) Fetch(file *storage.File, opt storage.FetchOption) (storage.Stream, error) {
	if c.client == nil {
		return nil, errors.New("s3 client not ready")
	}
	input := &s3.GetObjectInput{Bucket: aws.String(c.setting.Bucket), Key: aws.String(objectPath(file))}
	if opt.Start > 0 || opt.End > 0 {
		if opt.End > 0 {
			input.Range = aws.String(fmt.Sprintf("bytes=%d-%d", opt.Start, opt.End))
		} else {
			input.Range = aws.String(fmt.Sprintf("bytes=%d-", opt.Start))
		}
	}
	out, err := c.client.GetObject(context.Background(), input)
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()

	tmp, err := os.CreateTemp("", "bamgoo-storage-s3-*")
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(tmp, out.Body); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return nil, err
	}
	if _, err := tmp.Seek(0, 0); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return nil, err
	}
	return &tempStream{file: tmp, path: tmp.Name()}, nil
}

func (c *s3Connection) Download(file *storage.File, opt storage.DownloadOption) (string, error) {
	if c.client == nil {
		return "", errors.New("s3 client not ready")
	}
	if opt.Target == "" {
		return "", errors.New("invalid target")
	}
	if st, err := os.Stat(opt.Target); err == nil && !st.IsDir() {
		return opt.Target, nil
	}
	if err := os.MkdirAll(path.Dir(opt.Target), 0o755); err != nil {
		return "", err
	}
	f, err := os.Create(opt.Target)
	if err != nil {
		return "", err
	}
	defer f.Close()

	downloader := manager.NewDownloader(c.client)
	_, err = downloader.Download(context.Background(), f, &s3.GetObjectInput{
		Bucket: aws.String(c.setting.Bucket),
		Key:    aws.String(objectPath(file)),
	})
	if err != nil {
		return "", err
	}
	return opt.Target, nil
}

func (c *s3Connection) Remove(file *storage.File, _ storage.RemoveOption) error {
	if c.client == nil {
		return errors.New("s3 client not ready")
	}
	_, err := c.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(c.setting.Bucket),
		Key:    aws.String(objectPath(file)),
	})
	return err
}

func (c *s3Connection) Browse(file *storage.File, opt storage.BrowseOption) (string, error) {
	if c.client == nil {
		return "", errors.New("s3 client not ready")
	}
	exp := opt.Expires
	if exp <= 0 {
		exp = time.Hour
	}
	presign := s3.NewPresignClient(c.client)
	out, err := presign.PresignGetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(c.setting.Bucket),
		Key:    aws.String(objectPath(file)),
	}, s3.WithPresignExpires(exp))
	if err != nil {
		return "", err
	}
	return out.URL, nil
}

func objectPath(file *storage.File) string {
	name := file.Key()
	if file.Type() != "" {
		name = fmt.Sprintf("%s.%s", file.Key(), file.Type())
	}
	return path.Join(file.Prefix(), name)
}

func (t *tempStream) Read(p []byte) (int, error) {
	return t.file.Read(p)
}

func (t *tempStream) Seek(offset int64, whence int) (int64, error) {
	return t.file.Seek(offset, whence)
}

func (t *tempStream) ReadAt(p []byte, off int64) (int, error) {
	return t.file.ReadAt(p, off)
}

func (t *tempStream) Close() error {
	err := t.file.Close()
	_ = os.Remove(t.path)
	return err
}
