package dist

const (
	MsgType_NewNodeJoined int = 10 // request from joining node to one of the existing node, viral
	MsgType_NodesInSystem int = 11 // responses from existing nodes
	MsgType_AddSource     int = 12
)

type DistMessage struct {
	Mtype int `json:"mtype"`
	Data  []byte `json:"data"`
}

type Node struct {
	Guuid string `json:"guuid"`
	Address string `json:"address"`
}
type NewNodeJoinedMsg struct {
	Node Node `json:"node"`
}

type NodesInSystemMsg struct {
	Nodes []Node
}

type AddSourceMsg struct {
	SourceScrapeTargetURL string //required by prometheus scrape config
	SourceGuuid string  // required by prometheus scrape config's static labels config
}
