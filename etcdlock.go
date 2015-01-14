package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/ecnahc515/etcdlock/etcd"
	"github.com/ecnahc515/etcdlock/lock"
)

var (
	endpoint string
)

func init() {
	flag.StringVar(&endpoint, "endpoint", "http://127.0.0.1:4001", "etcd endpoint for etcdlock. Defaults to the local instance.")
}

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		fmt.Println("No command specified. Use -h for help.")
		os.Exit(0)
	}

	if len(args) < 2 {
		fmt.Println("Must provide lock name")
		os.Exit(1)
	}
	name := args[1]

	etcdClient, err := etcd.NewEtcdClient([]string{endpoint}, nil)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	lockClient, err := lock.NewEtcdLockClient(etcdClient)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	l := lock.New(hostname, name, lockClient)

	switch args[0] {
	case "lock":
		err = l.Lock()
	case "unlock":
		err = l.Unlock()
	case "get":
		var sem *lock.Semaphore
		sem, err = l.Get()
		if err == nil {
			fmt.Println(sem.String())
		}
	}

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

}
