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
	"errors"
	"reflect"
	"testing"

	goetcd "github.com/coreos/go-etcd/etcd"
	"github.com/ecnahc515/etcdlock/etcd"
)

type testEtcdClient struct {
	err  error
	resp *goetcd.Response
}

func (t *testEtcdClient) Create(key string, value string, ttl uint64) (*goetcd.Response, error) {
	return t.resp, t.err
}

func (t *testEtcdClient) CreateDir(dir string, ttl uint64) (*goetcd.Response, error) {
	return t.resp, t.err
}

func (t *testEtcdClient) CompareAndSwap(key string, value string, ttl uint64, prevValue string, prevIndex uint64) (*goetcd.Response, error) {
	return t.resp, t.err
}

func (t *testEtcdClient) Get(key string, sort, recursive bool) (*goetcd.Response, error) {
	return t.resp, t.err
}

func TestEtcdLockClientInit(t *testing.T) {
	for i, tt := range []struct {
		ee   error
		want bool
	}{
		{nil, false},
		{&goetcd.EtcdError{ErrorCode: etcd.ErrorNodeExist}, false},
		{&goetcd.EtcdError{ErrorCode: etcd.ErrorKeyNotFound}, true},
		{errors.New("some random error"), true},
	} {
		elc := &EtcdLockClient{
			client: &testEtcdClient{err: tt.ee},
		}
		got := elc.Init()
		if (got != nil) != tt.want {
			t.Errorf("case %d: unexpected error state initializing Client: got %v", i, got)
		}
	}
}

func makeResponse(idx int, val string) *goetcd.Response {
	return &goetcd.Response{
		Node: &goetcd.Node{
			Value:         val,
			ModifiedIndex: uint64(idx),
		},
	}
}

func TestEtcdLockClientGet(t *testing.T) {
	for i, tt := range []struct {
		ee error
		er *goetcd.Response
		ws *Semaphore
		we bool
	}{
		// errors returned from etcd
		{errors.New("some error"), nil, nil, true},
		{&goetcd.EtcdError{ErrorCode: etcd.ErrorKeyNotFound}, nil, nil, true},
		// bad JSON should cause errors
		{nil, makeResponse(0, "asdf"), nil, true},
		{nil, makeResponse(0, `{"semaphore:`), nil, true},
		// successful calls
		{nil, makeResponse(10, `{"semaphore": 1}`), &Semaphore{Index: 10, Semaphore: 1}, false},
		{nil, makeResponse(1024, `{"semaphore": 1, "max": 2, "holders": ["foo", "bar"]}`), &Semaphore{Index: 1024, Semaphore: 1, Max: 2, Holders: []string{"foo", "bar"}}, false},
		// index should be set from etcd, not json!
		{nil, makeResponse(1234, `{"semaphore": 89, "index": 4567}`), &Semaphore{Index: 1234, Semaphore: 89}, false},
	} {
		elc := &EtcdLockClient{
			client: &testEtcdClient{
				err:  tt.ee,
				resp: tt.er,
			},
		}
		gs, ge := elc.Get("test")
		if tt.we {
			if ge == nil {
				t.Fatalf("case %d: expected error but got nil!", i)
			}
		} else {
			if ge != nil {
				t.Fatalf("case %d: unexpected error: %v", i, ge)
			}
		}
		if !reflect.DeepEqual(gs, tt.ws) {
			t.Fatalf("case %d: bad semaphore: got %#v, want %#v", i, gs, tt.ws)
		}
	}
}

func TestEtcdLockClientSet(t *testing.T) {
	for i, tt := range []struct {
		sem  *Semaphore
		ee   error // error returned from etcd
		want bool  // do we expect Set to return an error
	}{
		// nil semaphore cannot be set
		{nil, nil, true},
		// empty semaphore is OK
		{&Semaphore{}, nil, false},
		{&Semaphore{Index: uint64(1234)}, nil, false},
		// all errors returned from etcd should propagate
		{&Semaphore{}, &goetcd.EtcdError{ErrorCode: etcd.ErrorNodeExist}, true},
		{&Semaphore{}, &goetcd.EtcdError{ErrorCode: etcd.ErrorKeyNotFound}, true},
		{&Semaphore{}, errors.New("some random error"), true},
	} {
		elc := &EtcdLockClient{
			client: &testEtcdClient{err: tt.ee},
		}
		got := elc.Set("test", tt.sem)
		if (got != nil) != tt.want {
			t.Errorf("case %d: unexpected error state calling Set: got %v", i, got)
		}
	}
}
