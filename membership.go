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

package smudge

import (
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/tevino/abool"
)

// A scalar value used to calculate a variety of limits
const lambda = 2.5

// How many standard deviations beyond the mean PING/ACK response time we
// allow before timing out an ACK.
const timeoutToleranceSigmas = 3.0

var currentHeartbeat uint32

var pendingAcks = struct {
	sync.RWMutex
	m map[string]*pendingAck
}{m: make(map[string]*pendingAck)}

var thisHostAddress string

var thisHost *Node

// This flag is set whenever a known node is added or removed.
var knownNodesModifiedFlag = false

var pingdata = newPingData(150, 50)

// The smudge udp listen conn
var udpConn *net.UDPConn

// The smudge running flag
var runningFlag *abool.AtomicBool

/******************************************************************************
 * Exported functions (for public consumption)
 *****************************************************************************/

// Begin starts the server by opening a UDP port and beginning the heartbeat.
// Note that this is a blocking function, so act appropriately.
func Begin() {
	// Add this host.
	ip, err := GetLocalIP()
	if err != nil {
		logFatal("Could not get local ip:", err)
		return
	}

	if ip == nil {
		logWarn("Warning: Could not resolve host IP. Using 127.0.0.1")
		ip = []byte{127, 0, 0, 1}
	}

	me := Node{
		ip:         ip,
		port:       uint16(GetListenPort()),
		timestamp:  GetNowInMillis(),
		pingMillis: PingNoData,
	}

	thisHostAddress = me.Address()
	thisHost = &me

	logInfo("My host address:", thisHostAddress)

	go listenUDP(GetListenPort())

	// Add this node's status. Don't update any other node's statuses: they'll
	// report those back to us.
	updateNodeStatus(thisHost, StatusAlive, 0)
	AddNode(thisHost)

	runningFlag = abool.NewBool(true)
	// Add initial hosts as specified by the SMUDGE_INITIAL_HOSTS property
	for _, address := range GetInitialHosts() {
		n, err := CreateNodeByAddress(address)
		if err != nil {
			logfError("Could not create node %s: %v\n", address, err)
		} else {
			AddNode(n)
		}
	}

	go startTimeoutCheckLoop()

	// Loop over a randomized list of all known nodes (except for this host
	// node), pinging one at a time. If the knownNodesModifiedFlag is set to
	// true by AddNode() or RemoveNode(), the we get a fresh list and start
	// again.

	for {
		var randomAllNodes = knownNodes.getRandomNodes(0, thisHost)
		var pingCounter int

		// Exponential backoff of dead nodes, until such time as they are removed.
		for _, node := range randomAllNodes {
			if !runningFlag.IsSet() {
				break
			}
			// Exponential backoff of dead nodes, until such time as they are removed.
			if node.status == StatusDead {
				var dnc *deadNodeCounter
				var ok bool

				deadNodeRetries.Lock()
				if dnc, ok = deadNodeRetries.m[node.Address()]; !ok {
					dnc = &deadNodeCounter{retry: 1, retryCountdown: 2}
					deadNodeRetries.m[node.Address()] = dnc
				}
				deadNodeRetries.Unlock()

				dnc.retryCountdown--

				if dnc.retryCountdown <= 0 {
					dnc.retry++
					dnc.retryCountdown = int(math.Pow(2.0, float64(dnc.retry)))

					if dnc.retry > maxDeadNodeRetries {
						logDebug("Forgetting dead node", node.Address())

						deadNodeRetries.Lock()
						delete(deadNodeRetries.m, node.Address())
						deadNodeRetries.Unlock()

						RemoveNode(node)
						continue
					}
				} else {
					continue
				}
			}

			currentHeartbeat++

			logfDebug("%d - hosts=%d (announce=%d forward=%d)\n",
				currentHeartbeat,
				len(randomAllNodes),
				emitCount(),
				pingRequestCount())

			PingNode(node)
			pingCounter++

			time.Sleep(time.Millisecond * time.Duration(GetHeartbeatMillis()))

			if knownNodesModifiedFlag {
				knownNodesModifiedFlag = false
				break
			}
		}
		if !runningFlag.IsSet() {
			break
		}

		if pingCounter == 0 {
			logDebug("No nodes to ping. So lonely. :(")
			time.Sleep(time.Millisecond * time.Duration(GetHeartbeatMillis()))
		}
	}
}

// Stop the server. close the udp lesten and stop the heartbeat.
func Stop() {
	if udpConn != nil {
		udpConn.SetDeadline(time.Now())
	} else {
		logError("udp Listen conn is nil")
	}
	runningFlag.UnSet()
}

// PingNode can be used to explicitly ping a node. Calls the low-level
// doPingNode(), and outputs a message (and returns an error) if it fails.
func PingNode(node *Node) error {
	err := transmitVerbPingUDP(node, currentHeartbeat)
	if err != nil {
		logInfo("Failure to ping", node, "->", err)
	}

	return err
}

/******************************************************************************
 * Private functions (for internal use only)
 *****************************************************************************/

// The number of times any node's new status should be emitted after changes.
// Currently set to (lambda * log(node count)).
func emitCount() int {
	logn := math.Log(float64(knownNodes.length()))
	mult := (lambda * logn) + 0.5

	return int(mult)
}

func doForwardOnTimeout(pack *pendingAck) {
	filteredNodes := getTargetNodes(pingRequestCount(), thisHost, pack.node)

	if len(filteredNodes) == 0 {
		logDebug(thisHost.Address(), "Cannot forward ping request: no more nodes")

		updateNodeStatus(pack.node, StatusDead, currentHeartbeat)
	} else {
		for i, n := range filteredNodes {
			logfDebug("(%d/%d) Requesting indirect ping of %s via %s\n",
				i+1,
				len(filteredNodes),
				pack.node.Address(),
				n.Address())

			transmitVerbForwardUDP(n, pack.node, currentHeartbeat)
		}
	}
}

// Returns a random slice of valid ping/forward request targets; i.e., not
// this node, and not dead.
func getTargetNodes(count int, exclude ...*Node) []*Node {
	randomNodes := knownNodes.getRandomNodes(0, exclude...)
	filteredNodes := make([]*Node, 0, count)

	for _, n := range randomNodes {
		if len(filteredNodes) >= count {
			break
		}

		if n.status == StatusDead {
			continue
		}

		filteredNodes = append(filteredNodes, n)
	}

	return filteredNodes
}

func listenUDP(port int) error {
	var err error
	listenAddress, err := net.ResolveUDPAddr("udp", ":"+strconv.FormatInt(int64(port), 10))
	if err != nil {
		return err
	}

	/* Now listen at selected port */
	udpConn, err = net.ListenUDP("udp", listenAddress)
	if err != nil {
		return err
	}
	defer udpConn.Close()

	for {
		buf := make([]byte, 512)
		n, addr, err := udpConn.ReadFromUDP(buf)
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
				logError("UDP Listen closes")
				return err
			}
			logError("UDP read error: ", err)
		}

		go func(addr *net.UDPAddr, msg []byte) {
			err = receiveMessageUDP(addr, buf[0:n])
			if err != nil {
				logError(err)
			}
		}(addr, buf[0:n])
	}
}

// The number of nodes to send a PINGREQ to when a PING times out.
// Currently set to (lambda * log(node count)).
func pingRequestCount() int {
	logn := math.Log(float64(knownNodes.length()))
	mult := (lambda * logn) + 0.5

	return int(mult)
}

func receiveMessageUDP(addr *net.UDPAddr, msgBytes []byte) error {
	msg, err := decodeMessage(addr.IP, msgBytes)
	if err != nil {
		return err
	}

	logfTrace("Got %v from %v code=%d\n",
		msg.verb,
		msg.sender.Address(),
		msg.senderHeartbeat)

	// Synchronize heartbeats
	if msg.senderHeartbeat > 0 && msg.senderHeartbeat-1 > currentHeartbeat {
		logfTrace("Heartbeat advanced from %d to %d\n",
			currentHeartbeat,
			msg.senderHeartbeat-1)

		currentHeartbeat = msg.senderHeartbeat - 1
	}

	updateStatusesFromMessage(msg)

	receiveBroadcast(msg.broadcast)

	// Handle the verb.
	switch msg.verb {
	case verbPing:
		err = receiveVerbPingUDP(msg)
	case verbAck:
		err = receiveVerbAckUDP(msg)
	case verbPingRequest:
		err = receiveVerbForwardUDP(msg)
	case verbNonForwardingPing:
		err = receiveVerbNonForwardPingUDP(msg)
	}

	if err != nil {
		return err
	}

	return nil
}

func receiveVerbAckUDP(msg message) error {
	key := msg.sender.Address() + ":" + strconv.FormatInt(int64(msg.senderHeartbeat), 10)

	pendingAcks.RLock()
	_, ok := pendingAcks.m[key]
	pendingAcks.RUnlock()

	if ok {
		msg.sender.Touch()

		pendingAcks.Lock()

		if pack, ok := pendingAcks.m[key]; ok {
			// If this is a response to a requested ping, respond to the
			// callback node
			if pack.callback != nil {
				go transmitVerbAckUDP(pack.callback, pack.callbackCode)
			} else {
				// Note the ping response time.
				notePingResponseTime(pack)
			}
		}

		delete(pendingAcks.m, key)
		pendingAcks.Unlock()
	}

	return nil
}

func notePingResponseTime(pack *pendingAck) {
	// Note the elapsed time
	elapsedMillis := pack.elapsed()

	pack.node.pingMillis = int(elapsedMillis)

	// For the purposes of timeout tolerance, we treat all pings less than
	// 10 as 10.
	if elapsedMillis < 10 {
		elapsedMillis = 10
	}

	pingdata.add(elapsedMillis)

	mean, stddev := pingdata.data()
	sigmas := pingdata.nSigma(timeoutToleranceSigmas)

	logfTrace("Got ACK in %dms (mean=%.02f stddev=%.02f sigmas=%.02f)\n",
		elapsedMillis,
		mean,
		stddev,
		sigmas)
}

func receiveVerbForwardUDP(msg message) error {
	// We don't forward to a node that we don't know.

	if len(msg.members) >= 0 &&
		msg.members[0].status == StatusForwardTo {

		member := msg.members[0]
		node := member.node
		code := member.heartbeat
		key := node.Address() + ":" + strconv.FormatInt(int64(code), 10)

		pack := pendingAck{
			node:         node,
			startTime:    GetNowInMillis(),
			callback:     msg.sender,
			callbackCode: code,
			packType:     packNFP}

		pendingAcks.Lock()
		pendingAcks.m[key] = &pack
		pendingAcks.Unlock()

		return transmitVerbGenericUDP(node, nil, verbNonForwardingPing, code)
	}

	return nil
}

func receiveVerbPingUDP(msg message) error {
	return transmitVerbAckUDP(msg.sender, msg.senderHeartbeat)
}

func receiveVerbNonForwardPingUDP(msg message) error {
	return transmitVerbAckUDP(msg.sender, msg.senderHeartbeat)
}

func startTimeoutCheckLoop() {
	for {
		pendingAcks.Lock()
		for k, pack := range pendingAcks.m {
			elapsed := pack.elapsed()
			timeoutMillis := uint32(pingdata.nSigma(timeoutToleranceSigmas))

			// Ping requests are expected to take quite a bit longer.
			// Just call it 2x for now.
			if pack.packType == packPingReq {
				timeoutMillis *= 2
			}

			// This pending ACK has taken longer than expected. Mark it as
			// timed out.
			if elapsed > timeoutMillis {
				switch pack.packType {
				case packPing:
					go doForwardOnTimeout(pack)
				case packPingReq:
					logDebug(k, "timed out after", timeoutMillis, "milliseconds (dropped PINGREQ)")

					if knownNodes.contains(pack.callback) {
						updateNodeStatus(pack.callback, StatusDead, currentHeartbeat)
						pack.callback.pingMillis = PingTimedOut
					}
				case packNFP:
					logDebug(k, "timed out after", timeoutMillis, "milliseconds (dropped NFP)")

					if knownNodes.contains(pack.node) {
						updateNodeStatus(pack.node, StatusDead, currentHeartbeat)
						pack.callback.pingMillis = PingTimedOut
					}
				}

				delete(pendingAcks.m, k)
			}
		}
		pendingAcks.Unlock()

		time.Sleep(time.Millisecond * 100)
	}
}

func transmitVerbGenericUDP(node *Node, forwardTo *Node, verb messageVerb, code uint32) error {
	// Transmit the ACK
	remoteAddr, err := net.ResolveUDPAddr("udp", node.Address())
	if err != nil {
		return err
	}

	c, err := net.DialUDP("udp", nil, remoteAddr)
	if err != nil {
		return err
	}
	defer c.Close()

	msg := newMessage(verb, thisHost, code)

	if forwardTo != nil {
		msg.addMember(forwardTo, StatusForwardTo, code)
	}

	// Add members for update.
	nodes := getRandomUpdatedNodes(pingRequestCount(), node, thisHost)

	// No updates to distribute? Send out a few updates on other known nodes.
	if len(nodes) == 0 {
		nodes = knownNodes.getRandomNodes(pingRequestCount(), node, thisHost)
	}

	for _, n := range nodes {
		err = msg.addMember(n, n.status, n.heartbeat)
		if err != nil {
			return err
		}

		n.emitCounter--
	}

	// Emit counters for broadcasts can be less than 0. We transmit positive
	// numbers, and decrement all the others. At some value < 0, the broadcast
	// is removed from the map all together.
	broadcast := getBroadcastToEmit()
	if broadcast != nil {
		if broadcast.emitCounter > 0 {
			msg.addBroadcast(broadcast)
		}

		broadcast.emitCounter--
	}

	_, err = c.Write(msg.encode())
	if err != nil {
		return err
	}

	// Decrement the update counters on those nodes
	for _, m := range msg.members {
		m.node.emitCounter--
	}

	logfTrace("Sent %v to %v\n", verb, node.Address())

	return nil
}

func transmitVerbForwardUDP(node *Node, downstream *Node, code uint32) error {
	key := node.Address() + ":" + strconv.FormatInt(int64(code), 10)

	pack := pendingAck{
		node:      node,
		startTime: GetNowInMillis(),
		callback:  downstream,
		packType:  packPingReq}

	pendingAcks.Lock()
	pendingAcks.m[key] = &pack
	pendingAcks.Unlock()

	return transmitVerbGenericUDP(node, downstream, verbPingRequest, code)
}

func transmitVerbAckUDP(node *Node, code uint32) error {
	return transmitVerbGenericUDP(node, nil, verbAck, code)
}

func transmitVerbPingUDP(node *Node, code uint32) error {
	key := node.Address() + ":" + strconv.FormatInt(int64(code), 10)
	pack := pendingAck{
		node:      node,
		startTime: GetNowInMillis(),
		packType:  packPing}

	pendingAcks.Lock()
	pendingAcks.m[key] = &pack
	pendingAcks.Unlock()

	return transmitVerbGenericUDP(node, nil, verbPing, code)
}

func updateStatusesFromMessage(msg message) {
	for _, m := range msg.members {
		// If the heartbeat in the message is less then the heartbeat
		// associated with the last known status, then we conclude that the
		// message is old and we drop it.
		if m.heartbeat < m.node.heartbeat {
			logfDebug("Message is old (%d vs %d): dropping\n",
				m.node.heartbeat, m.heartbeat)

			continue
		}

		switch m.status {
		case StatusForwardTo:
			// The FORWARD_TO status isn't useful here, so we ignore those
			continue
		case StatusDead:
			// Don't tell ME I'm dead.
			if m.node.Address() != thisHost.Address() {
				updateNodeStatus(m.node, m.status, m.heartbeat)
				AddNode(m.node)
			}
		default:
			updateNodeStatus(m.node, m.status, m.heartbeat)
			AddNode(m.node)
		}
	}

	// Obviously, we know the sender is alive. Report it as such.
	if msg.senderHeartbeat > msg.sender.heartbeat {
		updateNodeStatus(msg.sender, StatusAlive, msg.senderHeartbeat)
	}

	// First, if we don't know the sender, we add it.
	if !knownNodes.contains(msg.sender) {
		AddNode(msg.sender)
	}
}

// pendingAckType represents an expectation of a response to a previously
// emitted PING, PINGREQ, or NFP.
type pendingAck struct {
	startTime    uint32
	node         *Node
	callback     *Node
	callbackCode uint32
	packType     pendingAckType
}

func (a *pendingAck) elapsed() uint32 {
	return GetNowInMillis() - a.startTime
}

// pendingAckType represents the type of PING that a pendingAckType is waiting
// for a response for: PING, PINGREQ, or NFP.
type pendingAckType byte

const (
	packPing pendingAckType = iota
	packPingReq
	packNFP
)

func (p pendingAckType) String() string {
	switch p {
	case packPing:
		return "PING"
	case packPingReq:
		return "PINGREQ"
	case packNFP:
		return "NFP"
	default:
		return "UNDEFINED"
	}
}
