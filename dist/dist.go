package dist

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"net"
	"sort"
	"strconv"
	"sync"
	"path/filename"
	"github.com/prometheus/prometheus/handler/targets"
)

type Options struct {
	Id           string
	Longitude    string
	Latitude     string
	PeerAddress  string
	LocalAddress string
	ConfigFile	 string
}

type DistEngine struct {
	logger  log.Logger
	options *Options
	peers   map[string]Node //guuid versus ip:port
	mutex   sync.Mutex
}

func New(logger log.Logger, o *Options) *DistEngine {
	return &DistEngine{
		logger:  logger,
		options: o,
		peers:   make(map[string]Node),
	}
}

func (de *DistEngine) Start() {
	level.Info(de.logger).Log("msg", "Distribution Engine starting!")
	go de.startListener()

	if de.options.PeerAddress != "unspecified" {
		de.connectToPeer()
	}
}

func (de *DistEngine) startListener() {
	if tcpAddress, err := net.ResolveTCPAddr("tcp", de.options.LocalAddress); err != nil {
		level.Error(de.logger).Log("msg", "Error resolving address.", "error", err)
		return
	} else {
		if listener, listener_create_err := net.ListenTCP("tcp", tcpAddress); listener_create_err != nil {
			level.Error(de.logger).Log("msg", "Error creating listener.", "error", listener_create_err)
			return
		} else {
			for {
				if conn, accept_err := listener.Accept(); accept_err != nil {
					level.Error(de.logger).Log("msg", "Error in listen.", "error", accept_err)
					return
				} else {
					go de.connectionReaderJson(conn)
				}
			}
		}
	}
}
func (de *DistEngine) connectToPeer() {
	level.Info(de.logger).Log("msg", "Connecting to peer!")
	localNode := Node{
		Guuid:   de.options.Id,
		Address: de.options.LocalAddress,
	}
	newNodeJoinedMsg := NewNodeJoinedMsg{
		SenderNode: localNode,
		Node:       localNode,
	}
	if msgBytes, marshal_err := json.Marshal(newNodeJoinedMsg); marshal_err != nil {
		level.Error(de.logger).Log("msg", "Error marshalling new node join message to peer", "error", marshal_err)
	} else {
		distMessage := DistMessage{
			Mtype: MsgType_NewNodeJoined,
			Data:  msgBytes,
		}
		if send_err := de.sendMessage(de.options.PeerAddress, distMessage); send_err != nil {
			level.Error(de.logger).Log("msg", "Error sending node join message to peer", "error", send_err)
		}
	}
}

func (de *DistEngine) sendMessage(toAddress string, distMessage DistMessage) error {
	if conn, err := net.Dial("tcp", toAddress); err != nil {
		level.Error(de.logger).Log("msg", "Error connecting to peer.", "error", err)
	} else {
		encoder := json.NewEncoder(conn)
		if encode_err := encoder.Encode(distMessage); encode_err != nil {
			return err
		}
	}
	return nil
}

func (de *DistEngine) connectionReaderJson(conn net.Conn) {
	defer conn.Close()

	decoder := json.NewDecoder(conn)

	var distMessage DistMessage
	if decode_err := decoder.Decode(&distMessage); decode_err != nil {
		level.Error(de.logger).Log("msg", "Error decoding message.", "error", decode_err)
	} else {
		//level.Info(de.logger).Log("msg", "Received message.", "message type ", fmt.Sprintf("%d",distMessage.Mtype))
		switch {
		case distMessage.Mtype == MsgType_NewNodeJoined:
			if err := de.handleNewNodeJoined(distMessage.Data, distMessage); err != nil {
				level.Error(de.logger).Log("msg", "Error decoding NewNodeJoinedMsg.", "error", err)
			}
		case distMessage.Mtype == MsgType_NodesInSystem:
			if err := de.handleNodesInSystem(distMessage.Data); err != nil {
				level.Error(de.logger).Log("msg", "Error decoding NodesInSystemMsg.", "error", err)
			}
		case distMessage.Mtype == MsgType_AddSource:
			if err := de.handleAddSource(distMessage.Data); err != nil {
				level.Error(de.logger).Log("msg", "Error decoding AddSourceMsg.", "error", err)
			}
		default:
			level.Info(de.logger).Log("msg", "Unknown message received!")
		}
	}

}

func (de *DistEngine) handleNewNodeJoined(message []byte, originalJoinMsg DistMessage) error {
	var newNodeMsg NewNodeJoinedMsg
	if decode_err := json.Unmarshal(message, &newNodeMsg); decode_err != nil {
		return decode_err
	} else {
		level.Info(de.logger).Log("msg", fmt.Sprintf("Received new node joined event %+v", newNodeMsg))

		nodesInSystemMsg := NodesInSystemMsg{
			Nodes: make([]Node, 0),
		}
		nodesInSystemMsg.Nodes = append(nodesInSystemMsg.Nodes, Node{
			Guuid:   de.options.Id,
			Address: de.options.LocalAddress,
		})
		de.mutex.Lock()
		for _, node := range de.peers {
			nodesInSystemMsg.Nodes = append(nodesInSystemMsg.Nodes, node)
		}

		if msgBytes, marshal_err := json.Marshal(nodesInSystemMsg); marshal_err != nil {
			level.Error(de.logger).Log("msg", "Error marshalling Nodes In System Message", "error", marshal_err)
		} else {
			distMessage := DistMessage{
				Mtype: MsgType_NodesInSystem,
				Data:  msgBytes,
			}
			if send_err := de.sendMessage(newNodeMsg.Node.Address, distMessage); send_err != nil {
				level.Error(de.logger).Log("msg", "Error sending nodes in system event to joining node", "error", send_err)
			}
		}

		fwdedNewNodeJoinedMsg := NewNodeJoinedMsg{
			SenderNode: Node{
				Guuid:   de.options.Id,
				Address: de.options.LocalAddress,
			},
			Node:       newNodeMsg.Node,
		}
		if fwdedMsgBytes, fwd_marshal_err := json.Marshal(fwdedNewNodeJoinedMsg); fwd_marshal_err != nil {
			level.Error(de.logger).Log("msg", "Error marshalling fwded new node join message", "error", fwd_marshal_err)
		} else {
			distMessage := DistMessage{
				Mtype: MsgType_NewNodeJoined,
				Data:  fwdedMsgBytes,
			}
			for fwd_guid, fwd_node := range de.peers {
				if fwd_guid != newNodeMsg.SenderNode.Guuid {
					if fwd_send_err := de.sendMessage(fwd_node.Address, distMessage); fwd_send_err != nil {
						level.Error(de.logger).Log("msg", "Error fwding new node joined message", "error", fwd_send_err)
					}
				}
			}
		}

		de.peers[newNodeMsg.Node.Guuid] = newNodeMsg.Node
		de.mutex.Unlock()
		de.DumpPeers()
	}
	return nil
}

func (de *DistEngine) handleNodesInSystem(message []byte) error {
	var nodesInSystemMsg NodesInSystemMsg
	if decode_err := json.Unmarshal(message, &nodesInSystemMsg); decode_err != nil {
		return decode_err
	} else {
		level.Info(de.logger).Log("msg", fmt.Sprintf("Received nodes in system event %+v", nodesInSystemMsg))
		de.mutex.Lock()
		for _, node := range nodesInSystemMsg.Nodes {
			de.peers[node.Guuid] = node
		}
		de.mutex.Unlock()
		de.DumpPeers()
	}
	return nil
}

func (de *DistEngine) handleAddSource(message []byte) error {
	var addSourceMsg AddSourceMsg
	if decode_err := json.Unmarshal(message, &addSourceMsg); decode_err != nil {
		return decode_err
	} else {
		level.Info(de.logger).Log("msg", fmt.Sprintf("Received add source event %+v", addSourceMsg))

		yml_path = de.o.ConfigFile
		dir_target = filepath.Dir(yml_path)
		//TODO : add logic to update local prometheus config with new source info
		if !targets.AddTargetToConfig(addSourceMsg.SourceGuuid, addSourceMsg.SourceScrapeTargetURL, dir_target) {
			level.Error(de.logger).Log("msg", "Error adding target to this source")
		}
	}
	return nil
}

func (de * DistEngine) SendSourceAddToPeer(sourceGuid, sourceUrl, peerAddress string){
	addSourceMsg := AddSourceMsg{
		SourceScrapeTargetURL: sourceUrl,
		SourceGuuid:           sourceGuid,
	}
	if msgBytes, marshal_err := json.Marshal(addSourceMsg); marshal_err != nil {
		level.Error(de.logger).Log("msg", "Error marshalling add source message to peer", "error", marshal_err)
	} else {
		distMessage := DistMessage{
			Mtype: MsgType_AddSource,
			Data:  msgBytes,
		}
		if send_err := de.sendMessage(peerAddress, distMessage); send_err != nil {
			level.Error(de.logger).Log("msg", "Error sending add source message to peer", "error", send_err)
		}
	}
}

func (de *DistEngine) LookupGuid(inGuid string) Node {
	candidateNodes := make([]Node, 0)
	//TODO: mutex
	for _, node := range de.peers {
		candidateNodes = append(candidateNodes, node)
	}
	candidateNodes = append(candidateNodes, Node{
		Guuid:   de.options.Id,
		Address: de.options.LocalAddress,
	})
	sort.SliceStable(candidateNodes, func(i, j int) bool {
		return distanceBetween(candidateNodes[i].Guuid, inGuid) < distanceBetween(candidateNodes[j].Guuid, inGuid)
	})
	return candidateNodes[0]
}

func (de *DistEngine) GetLocalId() string {
	return de.options.Id
}

func (de *DistEngine) DumpPeers() {
	var buf bytes.Buffer
	buf.WriteString("Peers:")
	de.mutex.Lock()
	for guid, node := range de.peers {
		fmt.Fprintf(&buf, "\nGuid=%v Address=%v\n", guid, node.Address)
	}
	de.mutex.Unlock()
	level.Info(de.logger).Log("msg", buf.String())
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func distanceBetween(guid1, guid2 string) int64 {
	guid1Val, _ := strconv.ParseInt(guid1, 16, 64)
	guid2Val, _ := strconv.ParseInt(guid2, 16, 64)
	return min(abs(guid1Val-guid2Val), 4294967296-abs(guid1Val-guid2Val))
}

func getHexDifference(guid1, guid2 string) int64 {
	guid1Val, _ := strconv.ParseInt(guid1, 16, 64)
	guid2Val, _ := strconv.ParseInt(guid2, 16, 64)
	return guid1Val - guid2Val
}
