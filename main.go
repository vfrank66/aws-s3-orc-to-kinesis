package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	orc "github.com/scritchley/orc"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var (
	inputBucket = flag.String("bucket", "", "your bucket name")
	tableName   = flag.String("tableName", "", "your s3 prefix or table name in AWS Glue")
	stream      = flag.String("stream", "", "your stream name")
	region      = flag.String("region", "us-east-1", "your AWS region")
)

func main() {
	awsSess := setupAWS()
	kcClient, streamName := setupKinesisClient(awsSess)
	s3Client, bucket := setupS3Client(awsSess)

	s3Objects := retrieveS3Objects(s3Client, bucket, tableName)
	downloadedFileName := downloadS3File(awsSess, bucket, s3Objects[0].Key)
	jsonRows := readAndPrintOrcDatav2(&downloadedFileName)
	//os.Exit(0)
	for _, row := range jsonRows {
		putKinesisRecord(kcClient, streamName, row)
	}
	//putKinesisRecords(kcClient, streamName, jsonRows)
}
func setupAWS() *session.Session {
	sess := session.New(&aws.Config{Region: aws.String(*region)})
	return sess
}

func setupKinesisClient(sess *session.Session) (*kinesis.Kinesis, *string) {
	kc := kinesis.New(sess)
	streamName := aws.String(*stream)
	return kc, streamName
}
func setupS3Client(sess *session.Session) (*s3.S3, *string) {
	s3Client := s3.New(sess)
	if len(*inputBucket) < 1 {
		result, err := s3Client.ListBuckets(nil)
		if err != nil {
			exitErrorf("Unable to list buckets, %v", err)
		}

		fmt.Println("Buckets:")

		for _, b := range result.Buckets {
			fmt.Printf("* %s created on %s\n",
				aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
		}
		exitErrorf("Invalid input bucket")
	}
	bucket := aws.String(*inputBucket)
	return s3Client, bucket
}

func retrieveS3Objects(s3Client *s3.S3, bucket *string, tableName *string) []*s3.Object {
	input := &s3.ListObjectsV2Input{
		Bucket:  bucket,
		Prefix:  aws.String(*tableName),
		MaxKeys: aws.Int64(1000),
	}

	resp, err := s3Client.ListObjectsV2(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				fmt.Println(s3.ErrCodeNoSuchBucket, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return nil
	}

	fmt.Println("Total items in bucket: ", len(resp.Contents))

	fmt.Println("Printing top 3 objects")
	for i, item := range resp.Contents {
		fmt.Println("Name:         ", *item.Key)
		fmt.Println("Last modified:", *item.LastModified)
		fmt.Println("Size:         ", *item.Size)
		fmt.Println("")
		if i == 3 {
			break
		}
	}
	return resp.Contents
}

func downloadS3File(sess *session.Session, bucket *string, fileName *string) string {
	// dir, err := os.Getwd()
	// if err != nil {
	// 	log.Fatal(err)
	// }
	usableFileName := strings.ReplaceAll(*fileName, "/", "-")
	file, err := os.Create(usableFileName)
	if err != nil {
		exitErrorf("Unable to open file %q, %v", err)
	}

	defer file.Close()
	downloader := s3manager.NewDownloader(sess)
	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(*bucket),
			Key:    aws.String(*fileName),
		})
	if err != nil {
		exitErrorf("Unable to download item %q, %v", fileName, err)
	}

	fmt.Println("Downloaded", file.Name(), numBytes, "bytes")
	return usableFileName
}

func putKinesisRecord(kc *kinesis.Kinesis, streamName *string, data []byte) {

	putOutput, err := kc.PutRecord(&kinesis.PutRecordInput{
		Data:         data,
		StreamName:   streamName,
		PartitionKey: aws.String("test-partition1"),
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", putOutput)
}
func putKinesisRecords(kc *kinesis.Kinesis, streamName *string, data [][]byte) {
	var dataRecordsGrouped = make([][]*kinesis.PutRecordsRequestEntry, 0)
	// put 10 records using PutRecords API
	entries := make([]*kinesis.PutRecordsRequestEntry, 10)
	 i := 0
	for _, item := range data {
		
		if len(entries) < 10 {
			entries[i] = &kinesis.PutRecordsRequestEntry{
				Data:         item,
				PartitionKey: aws.String("test-partition1"),
			} 
			i++
		}
		if len(entries) == 10 {
			dataRecordsGrouped = append(dataRecordsGrouped, entries)
			i = 0
			entries = make([]*kinesis.PutRecordsRequestEntry, 10)
		}
		if len(dataRecordsGrouped) == 10 {
			break
		}
	}
	
		fmt.Printf("%v\n", dataRecordsGrouped)
	
	for _, records := range dataRecordsGrouped {
		putsOutput, err := kc.PutRecords(&kinesis.PutRecordsInput{
			Records:    records,
			StreamName: streamName,
		})
		if err != nil {
			panic(err)
		}

		// putsOutput has Records, and its shard id and sequece enumber.
		fmt.Printf("%v\n", putsOutput)
	}
}

func readAndPrintOrcDatav2(fileName *string) [][]byte {
	r, err := orc.Open(*fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	baseCol := r.Schema().Columns()
	fmt.Println("Un-nested columns in file:   ", baseCol)

	count := len(baseCol)
	values := make([]interface{}, count)
	scanArgs := make([]interface{}, count)
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var returnMapping [][]byte
	// select all the columns in the file to read from
	c := r.Select(baseCol...)
	// create a map of the un-nested column names to the row, index based
	// (nested columns will by default have their property names)
	intakeStructv2 := map[string]interface{}{}
	intakeStructv2["delta"] = "add"
	intakeStructv2["type"] = "member"
	masterData := make(map[string]interface{})
	for c.Stripes() {
		for c.Next() {
			currentRow := c.Row()
			for i, _ := range values {
				masterData[baseCol[i]] = currentRow[i]
			}
			
			intakeStructv2["data"] = masterData
			testjson, _ := json.Marshal(intakeStructv2)
			returnMapping = append(returnMapping, testjson)
		}
	}
	if err := c.Err(); err != nil {
		log.Fatal(err)
	}
	return returnMapping
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
