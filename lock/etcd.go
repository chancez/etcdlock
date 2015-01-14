/*
   Copyright 2015 CoreOS, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package lock

import (
	"encoding/json"
	"errors"

	goetcd "github.com/coreos/go-etcd/etcd"
	"github.com/ecnahc515/etcdlock/etcd"
)

const defaultKeyPrefix = "etcdlock"

// EtcdLockClient is a wrapper around the etcd client that provides
// simple primitives to operate on the internal semaphore and holders
// structs through etcd.
type EtcdLockClient struct {
	client    etcd.EtcdClient
	keyPrefix string
}

func NewEtcdLockClient(ec etcd.EtcdClient) (client *EtcdLockClient, err error) {
	client = &EtcdLockClient{client: ec}
	client.SetKeyPrefix(defaultKeyPrefix)
	err = client.Init()
	return
}

// Init sets an initial copy of the semaphore if it doesn't exist yet.
func (c *EtcdLockClient) Init() (err error) {
	if c.client == nil {
		c.client, err = etcd.NewEtcdClient([]string{"http://127.0.0.1:4001"}, nil)
		if err != nil {
			return
		}
	}
	_, err = c.client.CreateDir(c.keyPrefix, 0)
	if err != nil {
		eerr, ok := err.(*goetcd.EtcdError)
		if ok && eerr.ErrorCode == etcd.ErrorNodeExist {
			return nil
		}
	}

	return err
}

func (c *EtcdLockClient) newSemaphore(key string) (sem *Semaphore, err error) {
	sem = newSemaphore()
	b, err := json.Marshal(sem)
	if err != nil {
		return nil, err
	}
	_, err = c.client.Create(c.keyPrefix+"/"+key, string(b), 0)
	if err != nil {
		return nil, err
	}
	return sem, nil
}

func (c *EtcdLockClient) SetKeyPrefix(prefix string) {
	c.keyPrefix = prefix + "/semaphores"
}

// Get fetches the Semaphore from etcd.
func (c *EtcdLockClient) Get(key string) (sem *Semaphore, err error) {
	if key == "" {
		return nil, errors.New("cannot get empty key")
	}
	resp, err := c.client.Get(c.keyPrefix+"/"+key, false, false)
	if err != nil {
		// If the semaphore doesn't exist, create it
		if etcd.ErrIsNotFound(err) {
			return c.newSemaphore(key)
		}
		return nil, err
	}

	sem = &Semaphore{}
	err = json.Unmarshal([]byte(resp.Node.Value), sem)
	if err != nil {
		return nil, err
	}

	sem.Index = resp.Node.ModifiedIndex

	return sem, nil
}

// Set sets a Semaphore in etcd.
func (c *EtcdLockClient) Set(key string, sem *Semaphore) (err error) {
	if sem == nil {
		return errors.New("cannot set nil semaphore")
	}
	if key == "" {
		return errors.New("cannot set key to empty string")
	}

	b, err := json.Marshal(sem)
	if err != nil {
		return err
	}

	_, err = c.client.CompareAndSwap(c.keyPrefix+"/"+key, string(b), 0, "", sem.Index)
	return err
}
