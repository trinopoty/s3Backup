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
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"io"
	"log"
	"math"
	"net/url"
	"os"
	"path"
	"runtime"
	"strings"
	"time"
)

var WindowsSystemFiles = []string{"$RECYCLE.BIN", "desktop.ini"}
var DarwinSystemFiles = []string{".DS_Store"}

func FormatSize(size float64) string {
	suffixes := []string{"B", "KB", "MB", "GB", "TB"}
	idx := 0

	for {
		if size < 1024 || (idx+1) >= len(suffixes) {
			return fmt.Sprintf("%.2f %s", size, suffixes[idx])
		} else {
			size /= 1024
			idx += 1
		}
	}
}

type ProgressTrackingReader struct {
	io.ReadCloser

	totalSize       int64
	currentPosition int64
	lastRead        int
	lastReadAt      int64
}

func (pr *ProgressTrackingReader) ReportProgress() {
	pr.currentPosition += int64(pr.lastRead)
	pr.lastRead = 0
	pr.lastReadAt = time.Now().UnixMilli()

	percentDone := int(math.Floor(float64(100*pr.currentPosition) / float64(pr.totalSize)))
	fmt.Printf("\r[%s%s] %d %% (%s/%s)   ",
		strings.Repeat("=", percentDone),
		strings.Repeat(" ", 100-percentDone),
		percentDone,
		FormatSize(float64(pr.currentPosition)),
		FormatSize(float64(pr.totalSize)))
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

func NewProgressTrackingReader(file *os.File, totalSize int64) *ProgressTrackingReader {
	return &ProgressTrackingReader{file, totalSize, 0, 0, 0}
}

func main() {
	awsProfilePtr := flag.String("profile", "", "AWS profile (optional)")
	s3Accelerate := flag.Bool("accelerate", false, "Use S3 acceleration (optional)")
	forceHashCheck := flag.Bool("force-hash", false, "Force hash check (optional)")
	flag.Parse()

	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(1)
	}

	s3Client := CreateS3ClientFromArgs(awsProfilePtr, s3Accelerate)
	s3Host, s3Key, srcFile := SanitizePaths(flag.Arg(0), flag.Arg(1))

	Upload(s3Client, manager.NewUploader(s3Client), s3Host, s3Key, srcFile, *forceHashCheck)
}

func CreateS3ClientFromArgs(awsProfilePtr *string, s3Accelerate *bool) *s3.Client {
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

	return s3.NewFromConfig(cfg, s3Options...)
}

func SanitizePaths(source string, destination string) (s3Bucket string, s3Key string, srcFile string) {
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

	s3Key = s3Url.Path

	// If path ends with '/', target is inside the provided path, otherwise,
	// target is the path itself
	srcFile = source

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

	return s3Url.Host, s3Key, srcFile
}

func Upload(s3Client *s3.Client, s3Uploader *manager.Uploader, bucketName string, s3Key string, srcFile string, forceHashCheck bool) {
	ignoredNames := map[string]bool{}
	if runtime.GOOS == "windows" {
		for _, item := range WindowsSystemFiles {
			ignoredNames[item] = true
		}
	} else if runtime.GOOS == "darwin" {
		for _, item := range DarwinSystemFiles {
			ignoredNames[item] = true
		}
	}

	if strings.HasPrefix(s3Key, "/") {
		s3Key = s3Key[1:]
	}

	if info, err := os.Stat(srcFile); err != nil {
		fmt.Printf("Unable to get information on %s\n", srcFile)
	} else if info.IsDir() {
		if files, err := os.ReadDir(srcFile); err == nil {
			for _, f := range files {
				f := f.Name()
				Upload(s3Client, s3Uploader, bucketName, s3Key+"/"+f, path.Join(srcFile, f), forceHashCheck)
			}
		} else {
			fmt.Printf("Unable to get files in directory %s\n", srcFile)
		}
	} else if info.Mode()&os.ModeType != 0 {
		fmt.Printf("%s is an irregular file. Skipping...\n", srcFile)
	} else if _, ignoredName := ignoredNames[info.Name()]; ignoredName {
		fmt.Printf("%s is a restricted file. Skipping...\n", srcFile)
	} else {
		UploadFile(s3Client, s3Uploader, bucketName, s3Key, srcFile, forceHashCheck, info)
	}
}

func UploadFile(s3Client *s3.Client, s3Uploader *manager.Uploader, bucketName string, s3Key string, srcFile string, forceHashCheck bool, info os.FileInfo) {
	fmt.Printf("Uploading %s to %s\n", srcFile, s3Key)

	modTime := info.ModTime().Format("2006-01-02 15:04:05")

	var exists = false
	var tagResponse *s3.GetObjectTaggingOutput = nil

	headObjectResponse, err := s3Client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(s3Key),
	})
	if err != nil {
		var oe *smithy.OperationError
		var s3Err *http.ResponseError

		if !errors.As(err, &oe) || !errors.As(oe.Err, &s3Err) || s3Err.Response.StatusCode != 404 {
			fmt.Printf("Unable to retrieve s3 object metadata for key %s [%s]\n", s3Key, err.Error())
			return
		}
	} else if headObjectResponse.DeleteMarker == nil && info.Size() == *headObjectResponse.ContentLength {
		tagResponse, _ = s3Client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(s3Key),
		})
		if !forceHashCheck && tagResponse != nil {
			for _, tag := range tagResponse.TagSet {
				if *tag.Key == "modified-timestamp" {
					exists = exists || (*tag.Value == modTime)
					break
				}
			}
		}
	}

	if exists {
		fmt.Printf("%s already exists. Skipping...\n", s3Key)
		return
	}

	hash, err := GetFileHash(srcFile)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if headObjectResponse != nil && headObjectResponse.DeleteMarker == nil && info.Size() == *headObjectResponse.ContentLength {
		if val, ok := headObjectResponse.Metadata["sha256"]; ok {
			exists = exists || (val == *hash)
		}
	}

	metadata := make(map[string]string)
	metadata["sha256"] = *hash

	var tags = make([]types.Tag, 0)
	if tagResponse != nil {
		for _, tag := range tagResponse.TagSet {
			if *tag.Key != "modified-timestamp" {
				tags = append(tags, tag)
			}
		}
	}
	tags = append(tags, types.Tag{
		Key:   aws.String("modified-timestamp"),
		Value: aws.String(modTime),
	})

	if !exists {
		// Upload file
		if f, err := os.Open(srcFile); err != nil {
			fmt.Printf("Unable to open file %s\n", srcFile)
			return
		} else {
			defer f.Close()

			s3Body := NewProgressTrackingReader(f, info.Size())
			_, err := s3Uploader.Upload(context.TODO(), &s3.PutObjectInput{
				Bucket:   aws.String(bucketName),
				Key:      aws.String(s3Key),
				Body:     s3Body,
				Metadata: metadata,
			})

			fmt.Printf("\n")
			if err != nil {
				fmt.Printf("Unable to upload %s\n", srcFile)
				return
			}
		}
	} else {
		// Update metadata
		fmt.Printf("%s already exists. Updating tags...\n", s3Key)
	}

	_, err = s3Client.PutObjectTagging(context.TODO(), &s3.PutObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(s3Key),
		Tagging: &types.Tagging{
			TagSet: tags,
		},
	})
	if err != nil {
		fmt.Printf("Unable to update tag for %s\n", s3Key)
	}
}

func GetFileHash(file string) (*string, error) {
	if f, err := os.Open(file); err != nil {
		return nil, errors.New(fmt.Sprintf("Unable to open file %s\n", file))
	} else {
		defer f.Close()

		hash := sha256.New()
		if _, err := io.Copy(hash, f); err != nil {
			return nil, errors.New(fmt.Sprintf("Unable to read file %s", file))
		} else {
			hash := fmt.Sprintf("%x", hash.Sum(nil))
			return &hash, nil
		}
	}
}
