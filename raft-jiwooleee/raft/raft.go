package raft

// Raft implementation.

// This file is divided into the following sections, each containing functions
// that implement the roles described in the Raft paper.

// Background Election Thread -------------------------------------------------
// Perioidic thread that checks whether this server should start an election.
// Also attempts to commit any outstanding entries, if necessary (this could be
// a separate periodic thread but is included here).

// Background Leader Thread ---------------------------------------------------
// Periodic thread that performs leadership duties if leader. The leader must
// 1) call the AppendEntries RPC of its followers (either with empty messages or
// with entries to append) and 2) check whether the commit index has advanced.

// Start an Election ----------------------------------------------------------
// Functions to start an election by sending RequestVote RPCs to followers.

// Send AppendEntries RPCs to Followers ---------------------------------------
// Send followers AppendEntries, either as heartbeat or with state machine
// changes.

// Incoming RPC Handlers ------------------------------------------------------
// Handlers for incoming AppendEntries and RequestVote requests. Should populate
// reply based on the current state.

// Commit-Related Functions ---------------------------------------------------
// Functions to update the leader commit index based on the match indices of all
// followers, as well as to apply all outstanding commits.

// Persistence Functions ------------------------------------------------------
// Functions to persist and to read from persistent state. recoverState should
// only be called upon recovery (in CreateRaft). persist should be called
// whenever the durable state changes.

// Outgoing RPC Handlers ------------------------------------------------------
// Call these to invoke a given RPC on the given client.

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"time"

	"github.com/golang/glog"
)

// buffer struct for persist() and recoverState()
type PersistedStates struct {
	Term, VotedFor int
	Log            []LogEntry
}

// Resets the election timer. Randomizes timeout to between 0.5 * election
// timeout and 1.5 * election timeout.
// REQUIRES r.mu
func (r *Raft) resetTimer() {
	to := int64(RaftElectionTimeout) / 2
	d := time.Duration(rand.Int63()%(2*to) + to)
	r.deadline = time.Now().Add(d)
}

// getStateLocked returns the current state while locked.
// REQUIRES r.mu
func (r *Raft) getStateLocked() (int, bool) {
	return r.currentTerm, r.state == leader
}

// Background Election Thread -------------------------------------------------
// Perioidic thread that checks whether this server should start an election.
// Also attempts to commit any outstanding entries, if necessary (this could be
// a separate periodic thread but is included here).

// Called once every n ms, where n is < election timeout. Checks whether the
// election has timed out. If so, starts an election. If not, returns
// immediately.
// EXCLUDES r.mu
func (r *Raft) electOnce() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopping {
		// Stopping, quit.
		return false
	}

	if time.Now().Before(r.deadline) {
		// Election deadline hasn't passed. Try later.
		// See if we can commit.
		go r.commit()
		return true
	}

	// Deadline has passed. Convert to candidate and start an election.
	glog.V(1).Infof("%d starting election for term %d", r.me, r.currentTerm+1)

	// 1: Increment the current term
	r.currentTerm++

	// 2: Vote for self
	r.state = candidate
	r.votedFor = r.me
	r.votes = 1
	r.persist()

	// 3: Reset election timer
	r.resetTimer()

	// 4: Send RequestVote RPCs to followers.
	r.sendBallots()

	return true
}

// Background Leader Thread ---------------------------------------------------
// Periodic thread that performs leadership duties if leader. The leader must:
// call AppendEntries on its followers (either with empty messages or with
// entries to append) and check whether the commit index has advanced.

// Called once every n ms, where n is much less than election timeout. If the
// process is a leader, performs leadership duties.
// EXCLUDES r.mu
func (r *Raft) leadOnce() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopping {
		// Stopping, quit.
		return false
	}

	if r.state != leader {
		// Not leader, try later.
		return true
	}

	// Reset deadline timer.
	r.resetTimer()
	// Send heartbeat messages.
	r.sendAppendEntries()
	// Update commitIndex, if possible.
	r.updateCommitIndex()

	return true
}

// Start an Election ----------------------------------------------------------
// Functions to start an election by sending RequestVote RPCs to followers.

// Send RequestVote RPCs to all peers.
// REQUIRES r.mu
func (r *Raft) sendBallots() {
	for p := range r.peers {
		if p == r.me {
			continue
		}

		LastIndex := len(r.log) - 1
		LastTerm := 0
		if LastIndex >= 0 {
			LastTerm = (r.log[LastIndex]).Term
		}
		args := requestVoteArgs{
			Term:         r.currentTerm,
			CandidateID:  r.me,
			LastLogTerm:  LastTerm,
			LastLogIndex: LastIndex,
		}
		go r.sendBallot(p, args)
	}
}

// Send an individual ballot to a peer; upon completion, check if we are now the
// leader. If so, become leader.
// EXCLUDES r.mu
func (r *Raft) sendBallot(peer int, args requestVoteArgs) {
	// Call RequestVote on peer.
	var reply requestVoteReply
	glog.V(8).Infof("Sending ballot to peer %d", peer)
	if ok := r.callRequestVote(peer, args, &reply); !ok {
		glog.V(6).Infof("RPC to %d failed.", peer)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// my state changed
	if r.state != candidate {
		glog.V(8).Infof("Peer %d state changed while waiting for VoteReply", r.me)
		return
	}

	// 1. if follower term > my term, I become a follower
	if reply.Term > args.Term {
		r.state = follower
		r.votedFor = -1
		r.currentTerm = reply.Term
		r.resetTimer()
		r.persist()
		glog.V(8).Infof("Peer %d state changed to follower: follower term > my term", r.me)
		return
	} else if reply.Term == args.Term {
		// 2. if the follower voted for me, check how many votes I have
		if reply.VoteGranted {
			r.votes++
			glog.V(8).Infof("One vote for candidate %d, total votes: %d", r.me, r.votes)
			if r.votes >= (len(r.peers)+1)/2 {
				r.state = leader
				// initialize nextIndex and matchIndex
				r.nextIndex = make([]int, 100)
				r.matchIndex = make([]int, 100)
				for p := range r.peers {
					r.nextIndex[p] = len(r.log)
					r.matchIndex[p] = 0
				}
				go r.leadOnce()
				glog.V(8).Infof("Won the election: candidate %d is now the leader for term %d", r.me, r.currentTerm)
				return
			}
		}
	}
	r.persist()
	return
}

// Send AppendEntries RPCs to Followers ---------------------------------------
// Send followers AppendEntries, either as heartbeat or with state machine
// changes.

// Send AppendEntries RPCs to all followers.
// REQUIRES r.mu
func (r *Raft) sendAppendEntries() {
	for p := range r.peers {
		if p == r.me {
			continue
		}

		PrevIndex := r.nextIndex[p] - 1
		PrevTerm := -1
		if PrevIndex >= 0 {
			PrevTerm = (r.log[PrevIndex]).Term
		}
		entries := r.log[r.nextIndex[p]:]
		args := appendEntriesArgs{
			Term:         r.currentTerm,
			LeaderID:     r.me,
			PrevLogTerm:  PrevTerm,
			PrevLogIndex: PrevIndex,
			Entries:      entries,
			LeaderCommit: r.commitIndex,
		}
		go r.sendAppendEntry(p, args)
	}
}

// Send a single AppendEntries RPC to a follower. After it returns, update state
// based on the response.
// EXCLUDES r.mu
func (r *Raft) sendAppendEntry(peer int, args appendEntriesArgs) {
	// Call AppendEntries on peer.
	glog.V(6).Infof("%d calling ae on %d", r.me, peer)
	var reply appendEntriesReply
	if ok := r.callAppendEntries(peer, args, &reply); !ok {
		glog.V(6).Infof("RPC to %d failed.", peer)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// 1. if follower term > leader term, I become a follower
	if reply.Term > args.Term {
		r.state = follower
		r.votedFor = -1
		r.currentTerm = reply.Term
		r.resetTimer()
		r.persist()
		glog.V(8).Infof("New leader needed: follower term > my term")
		return
	}

	// 2. if reply.Success, update nextIndex and matchIndex
	if (r.state == leader) && (args.Term == reply.Term) {
		if reply.Success {
			r.nextIndex[peer] = args.PrevLogIndex + 1 + len(args.Entries)
			r.matchIndex[peer] = r.nextIndex[peer] - 1
			glog.V(7).Infof("Peer %d nextIndex:%d, matchIndex:%d", peer, r.nextIndex[peer], r.matchIndex[peer])
		} else {
			// reply returned false, update nextIndex
			r.nextIndex[peer] = reply.NextIndex
		}

		return
	}
}

// Incoming RPC Handlers ------------------------------------------------------
// Handlers for incoming AppendEntries and RequestVote requests. Should populate
// reply based on the current state.

// RequestVote RPC handler.
// EXCLUDES r.mu
func (r *Raft) RequestVote(args requestVoteArgs, reply *requestVoteReply) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopping {
		return
	}

	index := len(r.log) - 1
	term := 0
	if index >= 0 {
		term = r.log[index].Term
	}
	// 1. if the candidate's term > my term, I become a follower / update my term
	if args.Term > r.currentTerm {
		r.state = follower
		r.votedFor = -1
		r.currentTerm = args.Term
		r.resetTimer()
		r.persist()
	}

	// 2. to vote for this candidate,
	// - I haven't voted for anyone else
	// - The candidate's last log term > my last log term OR it has more logs
	if (args.Term == r.currentTerm) && (r.votedFor < 0 || r.votedFor == args.CandidateID) && (args.LastLogTerm > term || (args.LastLogTerm == term && args.LastLogIndex >= index)) {

		r.votedFor = args.CandidateID
		reply.VoteGranted = true
		r.resetTimer()
		r.persist()
		glog.V(8).Infof("I (peer %d) vote for candidate %d", r.me, args.CandidateID)
	} else {
		glog.V(8).Infof("I (peer %d) do not vote for candidate %d", r.me, args.CandidateID)
		reply.VoteGranted = false
	}
	reply.Term = r.currentTerm
	r.persist()
	return
}

// AppendEntries RPC handler.
// EXCLUDES r.mu
func (r *Raft) AppendEntries(args appendEntriesArgs, reply *appendEntriesReply) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopping {
		return
	}
	// 1. if leader term >= my term, update my term
	if args.Term > r.currentTerm {
		r.state = follower
		r.votedFor = -1
		r.currentTerm = args.Term
		r.resetTimer()
		r.persist()
	}

	reply.Success = false
	reply.NextIndex = len(r.log)

	// 2. if leader term == my term
	if args.Term == r.currentTerm {
		// and if im a candidate, i become a follower
		if r.state != follower {
			r.state = follower
			r.votedFor = -1
			r.currentTerm = args.Term
			r.resetTimer()
			r.persist()
		}
		r.resetTimer()

		// 2-1. reply.Success: PrevLogIndex == -1 OR is smaller than the length of my log AND terms are the same
		if (args.PrevLogIndex == -1) || (args.PrevLogIndex < len(r.log)) && (args.PrevLogTerm == (r.log[args.PrevLogIndex]).Term) {
			reply.Success = true

			// 3. iterate through the log & entries to find where to insert
			logIndex := args.PrevLogIndex + 1
			entriesIndex := 0
			for {
				if (logIndex >= len(r.log)) || (entriesIndex >= len(args.Entries)) {
					break
				}
				if r.log[logIndex].Term != (args.Entries[entriesIndex]).Term {
					break
				}
				logIndex++
				entriesIndex++
			}

			// 4. append the log entries
			if entriesIndex < len(args.Entries) {
				glog.V(7).Infof("Current log: %+v", r.log)
				glog.V(7).Infof("Inserting log: %+v", args.Entries[entriesIndex:])
				r.log = append(r.log[:logIndex], args.Entries[entriesIndex:]...)
				r.persist()
				reply.NextIndex = len(r.log)
				glog.V(7).Infof("NextIndex: %d", reply.NextIndex)
			}
			glog.V(7).Infof("entriesIndex: %d len(entries): %d", logIndex, len(args.Entries))

			// 5. set commit index = min(leaderCommit, index of last new entry)
			if args.LeaderCommit > r.commitIndex {
				if args.LeaderCommit >= (len(r.log) - 1) {
					r.commitIndex = len(r.log) - 1
				} else {
					r.commitIndex = args.LeaderCommit
				}
				glog.V(7).Infof("Peer %d commitIndex: %d", r.me, r.commitIndex)
			}
		} else {
			// 2-2: !reply.Success: conflict at PrevLogIndex
			// 2-2-1: PrevLogIndex >= len(my log)
			if args.PrevLogIndex >= len(r.log) {
				reply.NextIndex = len(r.log)
			} else {
				// 2-2-2: term mismatch, find the first occurence of that term
				logTerm := r.log[args.PrevLogIndex].Term
				i := args.PrevLogIndex - 1
				for {
					if i < 0 {
						break
					}
					if r.log[i].Term != logTerm {
						break
					}
					i--
				}
				reply.NextIndex = i + 1
			}
		}
	}

	reply.Term = r.currentTerm
	return
}

// Commit-Related Functions ---------------------------------------------------
// Functions to update the leader commit index based on the match indices of all
// followers, as well as to apply all outstanding commits.

// Update commitIndex for the leader based on the match indices of followers.
// REQUIRES r.mu
func (r *Raft) updateCommitIndex() {
	// for every log entry index i > commitIndex
	for i := r.commitIndex + 1; i < len(r.log); i++ {
		// is this entry's term == currentTerm
		if r.log[i].Term == r.currentTerm {
			count := 1
			// how many peers have committed it?
			for peer := range r.peers {
				if peer == r.me {
					count++
				}
				if r.matchIndex[peer] >= i {
					glog.V(6).Infof("matchIndex of peer %d > %d", peer, i)
					count++
				}
			}
			// majority, send it through the Apply Channel
			if count*2 > len(r.peers)+1 {
				glog.V(6).Infof("matchIndex of majority > %d", i)
				glog.V(6).Infof("Changing commitIndex to %d", i)
				r.commitIndex = i
			}
		}
	}
	return
}

// Commit any outstanding committable indices.
// EXCLUDES r.mu
func (r *Raft) commit() {
	r.mu.Lock()
	defer r.mu.Unlock()
	var logs []LogEntry
	lastApplied := r.lastApplied

	// if we have logs to send
	if r.commitIndex > r.lastApplied {
		logs = r.log[(r.lastApplied + 1):(r.commitIndex + 1)]
		r.lastApplied = r.commitIndex

		// send them across the channel
		for index, log := range logs {
			r.apply <- ApplyMsg{
				CommandIndex: lastApplied + 1 + index,
				Command:      log.Command,
			}
		}
	}

	return
}

// Persistence Functions ------------------------------------------------------
// Functions to persist and to read from persistent state. recoverState should
// only be called upon recovery (in CreateRaft). persist should be called
// whenever the durable state changes.

// Save persistent state so that it can be recovered from the persistence layer
// after process recovery.
// REQUIRES r.mu
func (r *Raft) persist() {
	if r.persister == nil {
		// Null persister; called in test.
		return
	}

	persisted := PersistedStates{
		Term:     r.currentTerm,
		VotedFor: r.votedFor,
		Log:      r.log,
	}

	buff := new(bytes.Buffer)
	e := gob.NewEncoder(buff)
	if err := e.Encode(persisted); err != nil {
		glog.Fatalln(err)
	}
	data := buff.Bytes()
	r.persister.WritePersistentState(data)

	return
}

// Called during recovery with previously-persisted state.
// REQUIRES r.mu
func (r *Raft) recoverState(data []byte) {
	if data == nil || len(data) < 1 { // Bootstrap.
		r.log = append(r.log, LogEntry{0, nil})
		r.persist()
		return
	}

	if r.persister == nil {
		// Null persister; called in test.
		return
	}
	var recovered PersistedStates
	buff := bytes.NewBuffer(data)
	d := gob.NewDecoder(buff)
	if err := d.Decode(&recovered); err != nil {
		glog.Fatalln(err)
	}

	r.currentTerm = recovered.Term
	r.votedFor = recovered.VotedFor
	r.log = recovered.Log
	glog.V(5).Infof("recovered peer %d, term: %d, votedFor: %d, log length: %d", r.me, r.currentTerm, r.votedFor, len(r.log))

	return
}

// Outgoing RPC Handlers ------------------------------------------------------
// Call these to invoke a given RPC on the given client.

// Call RequestVote on the given server. reply will be populated by the
// receiver server.
func (r *Raft) requestVote(server int, args requestVoteArgs, reply *requestVoteReply) {
	if ok := r.callRequestVote(server, args, reply); !ok {
		glog.V(6).Infof("RPC to %d failed.", server)
		return
	}
}

// Call AppendEntries on the given server. reply will be populated by the
// receiver server.
func (r *Raft) appendEntries(server int, args appendEntriesArgs, reply *appendEntriesReply) {
	if ok := r.callAppendEntries(server, args, reply); !ok {
		glog.V(6).Infof("RPC to %d failed.", server)
		return
	}
}
