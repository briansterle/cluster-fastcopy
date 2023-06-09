# cluster-fastcopy
copy data between hdfs clusters blazingly fast

## Motivation
- need the ability to copy files in hdfs across network boundaries
- in enterprises there are sometimes network firewalls and partitions that prevent directly copying between clusters with a tool like distcp
- cluster-fastcopy facilitates data copies between hdfs clusters in these scenarios
- a common use case is copying down production data into nonprod dev/test clusters to enable testing

## Performance

Copy 10 128MB files in 2.2 seconds

Sample response from POST /copy
```json
{
	"from": "/tmp/bench10x128/",
	"to": "/tmp/out/",
	"written": 1342177280,
	"filesRequested": 10,
	"filesCopied": 10,
	"copyFailures": [],
	"throughputMbps": 4859.414809991191,
	"elapsedSecs": 2.209611375
}
```

## API

Copy files in 'from' into 'to' on 'targetUrl'
```bash
curl --request POST \
  --url 'http://localhost:8080/copy?from=%2Ftmp%2Fbench32x128%2F&to=%2Ftmp%2Fout%2F&targetURL=http%3A%2F%2Flocalhost%3A8080%2Fupload'
```


Upload byte stream "hello, world" into 'to' directory with 'fileName'
```bash
curl --request POST \
  --url 'http://localhost:8080/upload?to=%2Ftmp%2Fin%2F&fileName=hello.txt' \
  --header 'Content-Type: application/octet-stream' \
  --data 'hello, world!'
```


## Flow
- receive a request to copy data from cluster1 to cluster2
- stream data from cluster1 into hdfs cluster2 by sending a byte stream to a microservice residing in cluster2's network partition
- make heavy use of goroutines to make this all as fast as possible


## dependencies
- https://github.com/colinmarc/hdfs/

