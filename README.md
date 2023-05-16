# cluster-fastcopy
copy data between hdfs clusters blazingly fast

# Motivation
- We want the ability to transfer files in hdfs over network boundaries
- Sometimes in enterprises there are network firewalls and partitions that prevent directly copying between clusters with a tool like HDFS
- The application will facilitate data copies between hdfs clusters in these sort of scenarios
- the most common use case will be copying down production data into nonprod dev/test clusters to facilitate testing


# Flow
the basic flow will be:

- recieve a request to copy data from cluster1 to cluster2
- read data from hdfs cluster1 into memory 
- batch write data into hdfs cluster2 by sending it to a microservice that resides in cluster2's network partition
- make heavy use of goroutines to make this all as fast as possible


# dependencies
- https://github.com/colinmarc/hdfs
