/*
Copyright 2016 The Smudge Authors.

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

package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/dotwoo/smudge"
)

func main() {
	var nodeAddress string
	var heartbeatMillis int
	var listenPort int
	var stopMinutes int
	var err error

	flag.StringVar(&nodeAddress, "node", "", "Initial node")

	flag.IntVar(&listenPort, "port",
		int(smudge.GetListenPort()),
		"The bind port")

	flag.IntVar(&heartbeatMillis, "hbf",
		int(smudge.GetHeartbeatMillis()),
		"The heartbeat frequency in milliseconds")

	flag.IntVar(&stopMinutes, "stop",
		0,
		"sleep some minutes then go to stop Smudge,default 0, not stop")

	flag.Parse()

	smudge.SetLogThreshold(smudge.LogInfo)
	smudge.SetListenPort(listenPort)
	myip, _ := smudge.GetLocalIP()
	smudge.SetListenIP(myip)
	smudge.SetHeartbeatMillis(heartbeatMillis)

	if nodeAddress != "" {
		node, err := smudge.CreateNodeByAddress(nodeAddress)

		if err == nil {
			smudge.AddNode(node)
		}
	}

	if err == nil {
		if stopMinutes > 0 {
			go func() {
				time.Sleep(time.Duration(stopMinutes*10) * time.Second)
				log.Println("smudge stop")
				smudge.Stop()
			}()
		}
		smudge.Begin()
	} else {
		fmt.Println(err)
	}
}
