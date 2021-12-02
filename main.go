package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gammazero/workerpool"
	"github.com/sirupsen/logrus"
)

var (
	endpoint = flag.String("endpoint", "", "s3 endpoint")
	region   = flag.String("region", "", "s3 region")
	bucket   = flag.String("bucket", "", "s3 bucket")

	accessKey = flag.String("access-key", "", "s3 access-key")
	secretKey = flag.String("secret-key", "", "s3 secret-key")

	workers = flag.Int("workers", 512, "Number of workers to use")
	buffer  = flag.Int("buffer", 32768, "Size of the buffer in number of objects")

	// debug = flag.Bool("debug", false, "debug mode")
)

func main() {
	flag.Parse()

	if *endpoint == "" || *region == "" || *bucket == "" || *accessKey == "" || *secretKey == "" {
		fmt.Println("<see usage>")
		os.Exit(1)
	}

	objsChan := make(chan string, *buffer)
	wp := workerpool.New(*workers)

	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(*accessKey, *secretKey, ""),
		Endpoint:    aws.String(*endpoint),
		Region:      aws.String(*region),
	})
	if err != nil {
		panic(err)
	}

	conn := s3.New(sess)

	for i := 1; i <= *workers; i++ {
		wp.Submit(func() {
			i := i
			for obj := range objsChan {
				DeleteObject(i, conn, *bucket, obj)
			}
		})
	}

	input := s3.ListObjectsV2Input{Bucket: aws.String(*bucket)}

	for {
		logrus.Info("list objects")
		output, err := conn.ListObjectsV2(&input)
		if err != nil {
			panic(err)
		}

		for _, obj := range output.Contents {
			objsChan <- *obj.Key
		}

		if output.NextContinuationToken != nil {
			input.ContinuationToken = output.NextContinuationToken
		} else {
			break
		}
	}
}

func DeleteObject(worker int, conn *s3.S3, bucket string, key string) {
	logrus.WithFields(logrus.Fields{
		"worker": worker,
		"key":    key,
		"bucket": bucket,
	}).Info("delete object")

	_, err := conn.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		logrus.Error(err)
	}
}
