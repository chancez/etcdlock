package etcd

import "github.com/coreos/go-etcd/etcd"

const (
	ErrorKeyNotFound = 100
	ErrorNodeExist   = 105
)

type EtcdClient interface {
	Create(key string, value string, ttl uint64) (*etcd.Response, error)
	CreateDir(dir string, ttl uint64) (*etcd.Response, error)
	CompareAndSwap(key string, value string, ttl uint64, prevValue string, prevIndex uint64) (*etcd.Response, error)
	Get(key string, sort, recursive bool) (*etcd.Response, error)
}

type TLSInfo struct {
	CertFile string
	KeyFile  string
	CAFile   string
}

// NewClient creates a new EtcdClient
func NewEtcdClient(machines []string, ti *TLSInfo) (EtcdClient, error) {
	if ti != nil {
		return etcd.NewTLSClient(machines, ti.CertFile, ti.KeyFile, ti.CAFile)
	}
	return etcd.NewClient(machines), nil
}

func ErrIsNotFound(err error) bool {
	if err, ok := err.(*etcd.EtcdError); ok {
		return err.ErrorCode == ErrorKeyNotFound
	}
	return false

}
