package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/minio/madmin-go/v3"
)

type Set struct {
	DiskCount  int
	SCParity   int
	RRSCParity int
	Set        int
	Pool       int
	CanReboot  bool
	Disks      []*Disk
}
type Disk struct {
	UUID   string
	Index  int
	Pool   int
	Server string
	Set    int
	Path   string
	State  string
}

var infra = make(map[string]*Set)

var (
	endpoint    string
	miniokey    string
	miniosecret string
)

func main() {
	flag.StringVar(&endpoint, "endpoint", "127.0.0.1:9000", "server endpoint")
	flag.StringVar(&miniokey, "key", "minioadmin", "minio user/key")
	flag.StringVar(&miniosecret, "secret", "minioadmin", "minio password/secret")
	flag.Parse()

	mclient, err := madmin.New(endpoint, miniokey, miniosecret, false)
	if err != nil {
		panic(err)
	}
	info, err := mclient.ServerInfo(context.Background(), func(sio *madmin.ServerInfoOpts) {
		fmt.Println(sio)
	})
	if err != nil {
		panic(err)
	}

	fullb, err := json.Marshal(info)
	if err != nil {
		panic(err)
	}

	ff, err := os.Create("infra.json")
	if err != nil {
		panic(err)
	}
	ff.Write(fullb)
	ff.Close()

	for _, v := range info.Servers {
		for _, vv := range v.Disks {
			index := fmt.Sprintf("%d-%d", vv.PoolIndex, vv.SetIndex)
			set, ok := infra[index]
			if !ok {
				infra[index] = &Set{
					SCParity:   info.Backend.StandardSCParity,
					RRSCParity: info.Backend.RRSCParity,
					Set:        vv.SetIndex,
					Pool:       vv.PoolIndex,
					CanReboot:  false,
					Disks:      make([]*Disk, 0),
				}
				set = infra[index]
			}
			set.Disks = append(set.Disks, &Disk{
				UUID:   vv.UUID,
				Index:  vv.DiskIndex,
				Pool:   vv.PoolIndex,
				Server: v.Endpoint,
				Set:    vv.SetIndex,
				Path:   vv.DrivePath,
				State:  vv.State,
			})
			set.DiskCount = len(set.Disks)
		}
	}
	for i, v := range infra {
		badDrives := 0
		for _, vv := range v.Disks {
			if vv.State != "ok" {
				badDrives++
			}
		}
		if badDrives > 0 {
			infra[i].CanReboot = false
		} else {
			infra[i].CanReboot = true
		}
	}

	for _, v := range infra {
		fmt.Println("Set:", v.Set, "Pool:", v.Pool, "CanReboot:", v.CanReboot)
		for _, vv := range v.Disks {
			if vv.State != "ok" {
				fmt.Println("offline:", vv.Server, vv.Path)
			}
		}
	}

	f, err := os.Create("sets.json")
	bb, err := json.Marshal(infra)
	if err != nil {
		panic(err)
	}
	f.Write(bb)
	f.Close()
}
