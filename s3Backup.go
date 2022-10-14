package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"io"
	"log"
	"math"
	"net/url"
	"os"
	"path"
	"strings"
	"time"
)

type ProgressFunc func(totalRead int64, speed float32)

type ProgressTrackingReader struct {
	io.ReadCloser

	currentPosition int64
	lastRead        int
	lastReadAt      int64
	ReportCallback  ProgressFunc
}

func (pr *ProgressTrackingReader) ReportProgress() {
	pr.currentPosition += int64(pr.lastRead)
	pr.lastRead = 0
	pr.lastReadAt = time.Now().UnixMilli()
	pr.ReportCallback(pr.currentPosition, 0)
}

func (pr *ProgressTrackingReader) Read(buff []byte) (int, error) {
	pr.ReportProgress()

	readCount, err := pr.ReadCloser.Read(buff)
	if err == nil {
		pr.lastRead = readCount
	}
	return readCount, err
}

func (pr *ProgressTrackingReader) Close() error {
	pr.ReportProgress()
	return pr.ReadCloser.Close()
}

func NewProgressTrackingReader(file *os.File, report ProgressFunc) *ProgressTrackingReader {
	return &ProgressTrackingReader{file, 0, 0, 0, report}
}

func main() {
	awsProfilePtr := flag.String("profile", "", "AWS profile (optional)")
	s3Accelerate := flag.Bool("accelerate", false, "Use S3 acceleration")
	flag.Parse()

	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(1)
	}

	var cfgOptions []func(*config.LoadOptions) error
	if awsProfilePtr != nil && len(*awsProfilePtr) > 0 {
		cfgOptions = []func(*config.LoadOptions) error{
			config.WithSharedConfigProfile(*awsProfilePtr),
		}
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), cfgOptions...)
	if err != nil {
		log.Fatal("Unable to create AWS config")
	}

	var s3Options []func(*s3.Options)
	if s3Accelerate != nil && *s3Accelerate {
		s3Options = []func(*s3.Options){
			func(options *s3.Options) {
				options.UseAccelerate = true
			},
		}
	}

	s3Client := s3.NewFromConfig(cfg, s3Options...)
	doBackup(s3Client, flag.Arg(0), flag.Arg(1))
}

func doBackup(s3Client *s3.Client, source string, destination string) {
	if !strings.HasPrefix(destination, "s3://") {
		fmt.Println("Backup destination is not s3")
		os.Exit(1)
	}

	if _, err := os.Stat(source); errors.Is(err, os.ErrNotExist) {
		fmt.Println("Backup source does not exist")
		os.Exit(1)
	}

	s3Url, err := url.Parse(destination)
	if err != nil {
		fmt.Println("Unable to parse backup destination path")
		os.Exit(1)
	}

	if len(s3Url.Host) <= 0 {
		fmt.Println("Unable to parse backup destination path")
		os.Exit(1)
	}

	s3Key := s3Url.Path

	// If path ends with '/', target is inside the provided path, otherwise,
	// target is the path itself
	srcFile := source

	s3IsDir := strings.HasSuffix(s3Key, "/")
	srcIsDir := strings.HasSuffix(srcFile, "/")

	if s3IsDir {
		if srcIsDir {
			s3Key = s3Key[:len(s3Key)-1]
			srcFile = srcFile[:len(srcFile)-1]
		} else {
			s3Key = s3Key + path.Base(srcFile)
		}
	} else {
		if srcIsDir {
			fmt.Printf("Cannot copy contents of directory %s to s3 path %s\n", srcFile, s3Key)
			os.Exit(1)
		}
	}

	Upload(s3Client, manager.NewUploader(s3Client), s3Url.Host, s3Key, srcFile)
}

func Upload(s3Client *s3.Client, s3Uploader *manager.Uploader, bucketName string, s3Key string, srcFile string) {
	if strings.HasPrefix(s3Key, "/") {
		s3Key = s3Key[1:]
	}

	if info, err := os.Stat(srcFile); err != nil {
		fmt.Printf("Unable to get information on %s\n", srcFile)
	} else if info.IsDir() {
		if files, err := os.ReadDir(srcFile); err == nil {
			for _, f := range files {
				f := f.Name()
				Upload(s3Client, s3Uploader, bucketName, s3Key+"/"+f, path.Join(srcFile, f))
			}
		} else {
			fmt.Printf("Unable to get files in directory %s\n", srcFile)
		}
	} else {
		fmt.Printf("Uploading %s to %s\n", srcFile, s3Key)

		if f, err := os.Open(srcFile); err != nil {
			fmt.Printf("Unable to open file %s\n", srcFile)
		} else {
			defer f.Close()

			hash := sha256.New()
			if _, err := io.Copy(hash, f); err != nil {
				fmt.Printf("Unable to read file %s", srcFile)
			} else {
				modTime := info.ModTime().Format("2006-01-02 15:04:05")
				hash := fmt.Sprintf("%x", hash.Sum(nil))

				exists := false
				if response, err := s3Client.HeadObject(context.TODO(), &s3.HeadObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(s3Key),
				}); err == nil {
					if !response.DeleteMarker {
						if val, ok := response.Metadata["modified-timestamp"]; ok {
							exists = exists || (val == modTime)
						}
						if val, ok := response.Metadata["sha256"]; ok {
							exists = exists || (val == hash)
						}
					}
				} else {
					var oe *smithy.OperationError
					var s3Err *http.ResponseError

					if !errors.As(err, &oe) || !errors.As(oe.Err, &s3Err) || s3Err.Response.StatusCode != 404 {
						fmt.Printf("Unable to retrieve s3 object metadata for key %s [%s]\n", s3Key, err.Error())
						return
					}
				}

				if !exists {
					f.Seek(0, io.SeekStart)

					metadata := make(map[string]string)
					metadata["modified-timestamp"] = modTime
					metadata["sha256"] = hash

					s3Body := NewProgressTrackingReader(f, func(totalRead int64, speed float32) {
						percentDone := int(math.Floor(float64(100*totalRead) / float64(info.Size())))
						fmt.Printf("\r[%s%s] %d %% (%s/%s)",
							strings.Repeat("=", percentDone),
							strings.Repeat(" ", 100-percentDone),
							percentDone,
							FormatSize(float64(totalRead)),
							FormatSize(float64(info.Size())))
					})

					_, err := s3Uploader.Upload(context.TODO(), &s3.PutObjectInput{
						Bucket:   aws.String(bucketName),
						Key:      aws.String(s3Key),
						Body:     s3Body,
						Metadata: metadata,
					})

					fmt.Printf("\n")
					if err != nil {
						fmt.Printf("Unable to upload %s\n", srcFile)
					}
				} else {
					fmt.Printf("%s already exists. Skipping...\n", s3Key)
				}
			}
		}
	}
}

func FormatSize(size float64) string {
	suffixes := []string{"B", "KB", "MB", "GB", "TB"}
	idx := 0

	for true {
		if size < 1024 || (idx+1) >= len(suffixes) {
			return fmt.Sprintf("%d %s", int(math.Floor(size)), suffixes[idx])
		} else {
			size /= 1024
			idx += 1
		}
	}

	return ""
}
