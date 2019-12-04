package dist

import (
	"encoding/json"
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"net"
	"sync"
)

type Options struct {
	Id           string
	Longitude    string
	Latitude     string
	PeerAddress  string
	LocalAddress string
}

type DistEngine struct {
	logger      log.Logger
	options     *Options
	connections map[string]net.TCPConn //address versus connection
	mutex       sync.Mutex
}

func New(logger log.Logger, o *Options) *DistEngine {
	return &DistEngine{
		logger:  logger,
		options: o,
	}
}

func (de *DistEngine) Start() {
	level.Info(de.logger).Log("msg", "Distribution Engine starting!")
	go de.startListener()
	level.Info(de.logger).Log("msg", "Started listener, now connecting to peer!")
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
	if conn, err := net.Dial("tcp", de.options.PeerAddress); err != nil {
		level.Error(de.logger).Log("msg", "Error connecting to peer.", "error", err)
	} else {
		go de.connectionReaderJson(conn)

		newNodeJoinedMsg := NewNodeJoinedMsg{
			Node: Node {
				Guuid:   de.options.Id,
				Address: de.options.LocalAddress,
			},
		}

		level.Info(de.logger).Log("msg", "Sending join message to peer", "message", fmt.Sprintf("%v", newNodeJoinedMsg))
		if msgBytes, marshal_err := json.Marshal(newNodeJoinedMsg); marshal_err != nil {
			level.Error(de.logger).Log("msg", "Error marshalling new node join message to peer", "error", marshal_err)
		} else {

			message := DistMessage{
				Mtype: MsgType_NewNodeJoined,
				Data:  msgBytes,
			}

			level.Info(de.logger).Log("msg", "Join message to peer", "message type/size", fmt.Sprintf("%d/%d", message.Mtype,len(message.Data)))
			encoder := json.NewEncoder(conn)

			if encode_err := encoder.Encode(message); encode_err != nil {
				level.Error(de.logger).Log("msg", "Error encoding new node join message to peer", "error", marshal_err)
			}
		}
	}
}

func sendMessage(conn net.Conn, data []byte) error {
	if _, err := conn.Write(data); err != nil {
		return err
	}

	return nil
}

func (de *DistEngine) connectionReaderJson(conn net.Conn) {
	defer conn.Close()

	decoder := json.NewDecoder(conn)
	for {
		var distMessage DistMessage
		if decode_err := decoder.Decode(&distMessage); decode_err != nil {
			level.Error(de.logger).Log("msg", "Error decoding message.", "error", decode_err)
		} else {
			level.Info(de.logger).Log("msg", "Received message.", "message type ", fmt.Sprintf("%d",distMessage.Mtype))
			switch {
			case distMessage.Mtype == MsgType_NewNodeJoined:
				if err := de.handleNewNodeJoined(distMessage.Data, conn); err != nil {
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
}

func (de *DistEngine) handleNewNodeJoined(message []byte, conn net.Conn) error {
	var newNodeMsg NewNodeJoinedMsg
	if decode_err := json.Unmarshal(message, &newNodeMsg); decode_err != nil {
		return decode_err
	} else {
		level.Info(de.logger).Log("msg", "Received new node joined event", "event", fmt.Sprintf("%v", newNodeMsg))
		//TODO : add new node added logic i.e. broadcast to all other nodes,
		// update own routes, respond with NodesInSystemMsg to joining node
	}
	return nil
}

func (de *DistEngine) handleNodesInSystem(message []byte) error {

	var nodesInSystem NodesInSystemMsg
	if decode_err := json.Unmarshal(message, &nodesInSystem); decode_err != nil {
		return decode_err
	} else {
		//TODO : add logic to update routes table with nodes in system
	}
	return nil
}

func (de *DistEngine) handleAddSource(message []byte) error {

	var addSourceMsg AddSourceMsg
	if decode_err := json.Unmarshal(message, &addSourceMsg); decode_err != nil {
		return decode_err
	} else {
		//TODO : add logic to update this prometheus config with new source info
	}
	return nil
}
