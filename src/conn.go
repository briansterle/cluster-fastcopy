package main

import (
	"fmt"
	"log"
	"os"

	"github.com/colinmarc/hdfs/v2"
	"github.com/colinmarc/hdfs/v2/hadoopconf"
	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
)

var HdfsClient *hdfs.Client

// lazy loads the global hdfs.Client
// for local testing, the env var HDFS_NAMENODE can be set (e.g. export HDFS_NAMENODE=localhost:9000)
// for production use with Kerberos, set $HADOOP_CONF_DIR to point at a dir with hdfs-site.xml and core-site.xml fie
// for kerberos props, set env vars RUNAS_USER to configure the kerberos principal and RUNAS_KEYTAB to configure the
// keytab to use for authentication
func GetHdfsClient() *hdfs.Client {
	if HdfsClient == nil {
		namenode := os.Getenv("HDFS_NAMENODE") // for basic local testing, set this env var
		fmt.Println(namenode)
		if namenode != "" {
			client, err := hdfs.New(namenode)
			if err != nil {
				log.Fatalf("failed to create hdfs client: %s", err)
			}
			HdfsClient = client
			return HdfsClient
		}
		conf, _ := hadoopconf.LoadFromEnvironment()
		opts := hdfs.ClientOptionsFromConf(conf)
		if os.Getenv("KRB_ENABLED") == "true" {
			opts.KerberosClient = makeKerberosClient()
		}
		client, err := hdfs.NewClient(opts)
		if err != nil {
			log.Fatalf("failed to create hdfs client: %s", err)
		}
		HdfsClient = client
	}
	return HdfsClient
}

// make a kerberos client. reads from env for configs.
func makeKerberosClient() *client.Client {
	kt, _ := keytab.Load(os.Getenv("KRB_KEYTAB"))
	file, _ := os.Open("/etc/krb5.conf")
	defer file.Close()
	krb5conf, _ := config.NewFromReader(file)
	return client.NewWithKeytab(os.Getenv("KRB_USER"), os.Getenv("KRB_REALM"), kt, krb5conf)
}
