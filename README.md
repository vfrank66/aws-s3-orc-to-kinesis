## Stream - Processor 

This is play project I have been using AWS Glue and creating .orc files, which made me really want to use github.com/scritchley/orc and golang.
Issues and comments are welcome as I learing golang. Data example was created with pythan faker.

This is a tooling support to assist in sending data to the kinesis intake stream. 

## Program Process 

1. using default credentials as AWS creds
2. setup kinesis and s3 clients for processing, validate bucket
3. download the first s3 file 
4. Read the orc data pulling all the data from the orc columns/rows
5. Using that rows create a json object
    a. add "delta" and "type" and put the row into a "data" json object format:
    ```json
    {
        "data": "",
        "type": "".
        "data": {...}
    }
    ```
6. For each new json object send the json object to kinesis stream
    a. print output from kinesis stream 

This is fast it appears to be a few dozen records a second, but no metrics have been gathered.

## Running the App
If using VSCode just F5 otherwise you can:

```go 
go run main.go -inputBucket -tableName -stream -region 
```

```bash
sh buildexe.sh
./main
```

Either was just remmeber there could be a LOT of data that starts processing and you might run out of compute power on your PC.