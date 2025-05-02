package chandy_lamport

import (
	"log"
	"sync"
)

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type localSnapshot struct {
	localTokens            int
	channelRecords         map[string][]*SnapshotMessage
	channelMarkersReceived map[string]bool
	pendingChannelCount    int
	complete               bool
}

type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	// TODO: ADD MORE FIELDS HERE

	snapshotRecords map[int]*localSnapshot
	snapshotLock    sync.Mutex
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		Id:              id,
		Tokens:          tokens,
		sim:             sim,
		outboundLinks:   make(map[string]*Link),
		inboundLinks:    make(map[string]*Link),
		snapshotRecords: make(map[int]*localSnapshot),
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	// TODO: IMPLEMENT ME
	server.snapshotLock.Lock()
	defer server.snapshotLock.Unlock()

	switch msg := message.(type) {
	case TokenMessage:
		for _, rec := range server.snapshotRecords {
			if !rec.channelMarkersReceived[src] && !rec.complete {
				rec.channelRecords[src] = append(
					rec.channelRecords[src],
					&SnapshotMessage{src, server.Id, msg},
				)
			}

		}
		server.Tokens += msg.numTokens

	case MarkerMessage:
		sid := msg.snapshotId
		rec, seen := server.snapshotRecords[sid]

		if !seen {
			rec = &localSnapshot{
				localTokens:            server.Tokens,
				channelRecords:         make(map[string][]*SnapshotMessage),
				channelMarkersReceived: make(map[string]bool),
				pendingChannelCount:    len(server.inboundLinks),
			}

			for ch := range server.inboundLinks {
				rec.channelRecords[ch] = make([]*SnapshotMessage, 0)
				rec.channelMarkersReceived[ch] = false
			}

			server.snapshotRecords[sid] = rec
			server.SendToNeighbors(msg)
		}
		if !rec.channelMarkersReceived[src] {
			rec.channelMarkersReceived[src] = true
			rec.pendingChannelCount--
		}

		if rec.pendingChannelCount == 0 && !rec.complete {
			rec.complete = true
			server.sim.NotifySnapshotComplete(server.Id, sid)
		}

	}
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME
	server.snapshotLock.Lock()
	defer server.snapshotLock.Unlock()

	if _, exist := server.snapshotRecords[snapshotId]; exist {
		return
	}

	rec := &localSnapshot{
		localTokens:            server.Tokens,
		channelRecords:         make(map[string][]*SnapshotMessage),
		channelMarkersReceived: make(map[string]bool),
		pendingChannelCount:    len(server.inboundLinks),
	}

	for ch := range server.inboundLinks {
		rec.channelRecords[ch] = make([]*SnapshotMessage, 0)
		rec.channelMarkersReceived[ch] = false
	}

	server.snapshotRecords[snapshotId] = rec

	server.sim.logger.RecordEvent(server, StartSnapshot{serverId: server.Id, snapshotId: snapshotId})

	server.SendToNeighbors(MarkerMessage{snapshotId})

	if rec.pendingChannelCount == 0 {
		rec.complete = true
		server.sim.NotifySnapshotComplete(server.Id, snapshotId)
	}

}
