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

const (
	keyPrefix       = "locks"
	SemaphorePrefix = keyPrefix + "/semaphores/"
)

// EtcdLockClient is a wrapper around the etcd client that provides
// simple primitives to operate on the internal semaphore and holders
// structs through etcd.
type EtcdLockClient struct {
	client etcd.EtcdClient
}

func NewEtcdLockClient(ec etcd.EtcdClient) (client *EtcdLockClient, err error) {
	client = &EtcdLockClient{ec}
	err = client.Init()
	return
}

// Init sets an initial copy of the semaphore if it doesn't exist yet.
func (c *EtcdLockClient) Init() (err error) {
	_, err = c.client.CreateDir(SemaphorePrefix, 0)
	if err != nil {
		eerr, ok := err.(*goetcd.EtcdError)
		if ok && eerr.ErrorCode == etcd.ErrorNodeExist {
			return nil
		}
	}

	return err
}

// Get fetches the Semaphore from etcd.
func (c *EtcdLockClient) Get(key string) (sem *Semaphore, err error) {
	if key == "" {
		return nil, errors.New("cannot get empty key")
	}
	resp, err := c.client.Get(SemaphorePrefix+key, false, false)
	if err != nil {
		eerr, ok := err.(*goetcd.EtcdError)
		// Key doesn't exist, create a new semaphore
		if ok && eerr.ErrorCode == etcd.ErrorKeyNotFound {
			sem = newSemaphore()
			b, err := json.Marshal(sem)
			if err != nil {
				return nil, err
			}
			_, err = c.client.Create(SemaphorePrefix+key, string(b), 0)
			if err != nil {
				return nil, err
			}
			return sem, nil
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

	_, err = c.client.CompareAndSwap(SemaphorePrefix+key, string(b), 0, "", sem.Index)
	// if err != nil {
	// 	eerr, ok := err.(*goetcd.EtcdError)
	// 	if ok && eerr.ErrorCode == ErrorKeyNotFound {
	// 		fmt.Println("What")
	// 		// sem := newSemaphore()
	// 		// b, err := json.Marshal(sem)
	// 		// if err != nil {
	// 		// 	return err
	// 		// }

	// 	}
	// }

	return err
}
