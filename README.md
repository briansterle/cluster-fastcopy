# cluster-fastcopy
copy data between hdfs clusters blazingly fast

# Motivation
- need the ability to copy files in hdfs across network boundaries
- in enterprises there are sometimes network firewalls and partitions that prevent directly copying between clusters with a tool like distcp
- cluster-fastcopy facilitates data copies between hdfs clusters in these scenarios
- a common use case is copying down production data into nonprod dev/test clusters to enable testing


# Flow
the basic flow will be:

- receive a request to copy data from cluster1 to cluster2
- read data from hdfs cluster1 into memory 
- batch write data into hdfs cluster2 by sending it to a microservice that resides in cluster2's network partition
- make heavy use of goroutines to make this all as fast as possible


# dependencies
- https://github.com/colinmarc/hdfs
