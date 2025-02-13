package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

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

func main() {
	mclient, err := madmin.New("127.0.0.1:9000", "minioadmin", "minioadmin", false)
	if err != nil {
		panic(err)
	}
	info, err := mclient.ServerInfo(context.Background(), func(sio *madmin.ServerInfoOpts) {
		fmt.Println(sio)
	})
	if err != nil {
		panic(err)
	}

	// fmt.Println(info.Backend.TotalSets)
	// fmt.Println(info.Backend.DrivesPerSet)
	// fmt.Println(info.Backend.StandardSCParity)
	// fmt.Println(info.Backend.RRSCParity)
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

	f, err := os.Create(fmt.Sprintf("%s-infra.json", time.Now().Format("2006-01-01-15-04-05-PM")))
	bb, err := json.Marshal(infra)
	if err != nil {
		panic(err)
	}
	f.Write(bb)
	f.Close()
}
