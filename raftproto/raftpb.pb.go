// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.30.0
// 	protoc        v3.20.1
// source: raftpb.proto

package raftproto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type EntryType int32

const (
	EntryType_EntryNormal     EntryType = 0
	EntryType_EntryConfChange EntryType = 1
)

// Enum value maps for EntryType.
var (
	EntryType_name = map[int32]string{
		0: "EntryNormal",
		1: "EntryConfChange",
	}
	EntryType_value = map[string]int32{
		"EntryNormal":     0,
		"EntryConfChange": 1,
	}
)

func (x EntryType) Enum() *EntryType {
	p := new(EntryType)
	*p = x
	return p
}

func (x EntryType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (EntryType) Descriptor() protoreflect.EnumDescriptor {
	return file_raftpb_proto_enumTypes[0].Descriptor()
}

func (EntryType) Type() protoreflect.EnumType {
	return &file_raftpb_proto_enumTypes[0]
}

func (x EntryType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use EntryType.Descriptor instead.
func (EntryType) EnumDescriptor() ([]byte, []int) {
	return file_raftpb_proto_rawDescGZIP(), []int{0}
}

// Some MessageType defined here are local messages which not come from the network, but should
// also use the Step method to handle
type MessageType int32

const (
	// 本地消息，用于请求节点开始选举
	MessageType_MsgHup MessageType = 0
	// 'MessageType_MsgBeat' is a local message that signals the leader to send a heartbeat
	// of the 'MessageType_MsgHeartbeat' type to its followers.
	MessageType_MsgBeat MessageType = 1
	// 'MessageType_MsgPropose' is a local message that proposes to append data to the leader's log entries.
	MessageType_MsgPropose MessageType = 2

	// 'MessageType_MsgAppend' contains log entries to replicate.
	MessageType_MsgAppend MessageType = 3
	// 'MessageType_MsgAppendResponse' is response to log replication request('MessageType_MsgAppend').
	MessageType_MsgAppendResponse MessageType = 4
	// 'MessageType_MsgRequestVote' requests votes for election.
	MessageType_MsgRequestVote MessageType = 5
	// 'MessageType_MsgRequestVoteResponse' contains responses from voting request.
	MessageType_MsgRequestVoteResponse MessageType = 6
	// 'MessageType_MsgSnapshot' requests to install a snapshot message.
	MessageType_MsgSnapshot MessageType = 7
	// 'MessageType_MsgHeartbeat' sends heartbeat from leader to its followers.
	MessageType_MsgHeartbeat MessageType = 8
	// 'MessageType_MsgHeartbeatResponse' is a response to 'MessageType_MsgHeartbeat'.
	MessageType_MsgHeartbeatResponse MessageType = 9
	// 'MessageType_MsgTransferLeader' requests the leader to transfer its leadership.
	MessageType_MsgTransferLeader MessageType = 11
	// 'MessageType_MsgTimeoutNow' send from the leader to the leadership transfer target, to let
	// the transfer target timeout immediately and start a new election.
	MessageType_MsgTimeoutNow MessageType = 12
)

// Enum value maps for MessageType.
var (
	MessageType_name = map[int32]string{
		0:  "MsgHup",
		1:  "MsgBeat",
		2:  "MsgPropose",
		3:  "MsgAppend",
		4:  "MsgAppendResponse",
		5:  "MsgRequestVote",
		6:  "MsgRequestVoteResponse",
		7:  "MsgSnapshot",
		8:  "MsgHeartbeat",
		9:  "MsgHeartbeatResponse",
		11: "MsgTransferLeader",
		12: "MsgTimeoutNow",
	}
	MessageType_value = map[string]int32{
		"MsgHup":                 0,
		"MsgBeat":                1,
		"MsgPropose":             2,
		"MsgAppend":              3,
		"MsgAppendResponse":      4,
		"MsgRequestVote":         5,
		"MsgRequestVoteResponse": 6,
		"MsgSnapshot":            7,
		"MsgHeartbeat":           8,
		"MsgHeartbeatResponse":   9,
		"MsgTransferLeader":      11,
		"MsgTimeoutNow":          12,
	}
)

func (x MessageType) Enum() *MessageType {
	p := new(MessageType)
	*p = x
	return p
}

func (x MessageType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (MessageType) Descriptor() protoreflect.EnumDescriptor {
	return file_raftpb_proto_enumTypes[1].Descriptor()
}

func (MessageType) Type() protoreflect.EnumType {
	return &file_raftpb_proto_enumTypes[1]
}

func (x MessageType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use MessageType.Descriptor instead.
func (MessageType) EnumDescriptor() ([]byte, []int) {
	return file_raftpb_proto_rawDescGZIP(), []int{1}
}

type ConfChangeType int32

const (
	ConfChangeType_AddNode    ConfChangeType = 0
	ConfChangeType_RemoveNode ConfChangeType = 1
)

// Enum value maps for ConfChangeType.
var (
	ConfChangeType_name = map[int32]string{
		0: "AddNode",
		1: "RemoveNode",
	}
	ConfChangeType_value = map[string]int32{
		"AddNode":    0,
		"RemoveNode": 1,
	}
)

func (x ConfChangeType) Enum() *ConfChangeType {
	p := new(ConfChangeType)
	*p = x
	return p
}

func (x ConfChangeType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ConfChangeType) Descriptor() protoreflect.EnumDescriptor {
	return file_raftpb_proto_enumTypes[2].Descriptor()
}

func (ConfChangeType) Type() protoreflect.EnumType {
	return &file_raftpb_proto_enumTypes[2]
}

func (x ConfChangeType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ConfChangeType.Descriptor instead.
func (ConfChangeType) EnumDescriptor() ([]byte, []int) {
	return file_raftpb_proto_rawDescGZIP(), []int{2}
}

type Entry struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	EntryType EntryType `protobuf:"varint,1,opt,name=entry_type,json=entryType,proto3,enum=raftproto.EntryType" json:"entry_type,omitempty"`
	Term      uint64    `protobuf:"varint,2,opt,name=term,proto3" json:"term,omitempty"`
	Index     uint64    `protobuf:"varint,3,opt,name=index,proto3" json:"index,omitempty"`
	Data      []byte    `protobuf:"bytes,4,opt,name=data,proto3" json:"data,omitempty"`
}

func (x *Entry) Reset() {
	*x = Entry{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raftpb_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Entry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Entry) ProtoMessage() {}

func (x *Entry) ProtoReflect() protoreflect.Message {
	mi := &file_raftpb_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Entry.ProtoReflect.Descriptor instead.
func (*Entry) Descriptor() ([]byte, []int) {
	return file_raftpb_proto_rawDescGZIP(), []int{0}
}

func (x *Entry) GetEntryType() EntryType {
	if x != nil {
		return x.EntryType
	}
	return EntryType_EntryNormal
}

func (x *Entry) GetTerm() uint64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *Entry) GetIndex() uint64 {
	if x != nil {
		return x.Index
	}
	return 0
}

func (x *Entry) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

// SnapshotMetadata contains the log index and term of the last log applied to this
// Snapshot, along with the membership information of the time the last log applied.
type SnapshotMetadata struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ConfState *ConfState `protobuf:"bytes,1,opt,name=conf_state,json=confState,proto3" json:"conf_state,omitempty"`
	Index     uint64     `protobuf:"varint,2,opt,name=index,proto3" json:"index,omitempty"`
	Term      uint64     `protobuf:"varint,3,opt,name=term,proto3" json:"term,omitempty"`
}

func (x *SnapshotMetadata) Reset() {
	*x = SnapshotMetadata{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raftpb_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SnapshotMetadata) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SnapshotMetadata) ProtoMessage() {}

func (x *SnapshotMetadata) ProtoReflect() protoreflect.Message {
	mi := &file_raftpb_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SnapshotMetadata.ProtoReflect.Descriptor instead.
func (*SnapshotMetadata) Descriptor() ([]byte, []int) {
	return file_raftpb_proto_rawDescGZIP(), []int{1}
}

func (x *SnapshotMetadata) GetConfState() *ConfState {
	if x != nil {
		return x.ConfState
	}
	return nil
}

func (x *SnapshotMetadata) GetIndex() uint64 {
	if x != nil {
		return x.Index
	}
	return 0
}

func (x *SnapshotMetadata) GetTerm() uint64 {
	if x != nil {
		return x.Term
	}
	return 0
}

type Snapshot struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Data     []byte            `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
	Metadata *SnapshotMetadata `protobuf:"bytes,2,opt,name=metadata,proto3" json:"metadata,omitempty"`
}

func (x *Snapshot) Reset() {
	*x = Snapshot{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raftpb_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Snapshot) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Snapshot) ProtoMessage() {}

func (x *Snapshot) ProtoReflect() protoreflect.Message {
	mi := &file_raftpb_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Snapshot.ProtoReflect.Descriptor instead.
func (*Snapshot) Descriptor() ([]byte, []int) {
	return file_raftpb_proto_rawDescGZIP(), []int{2}
}

func (x *Snapshot) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *Snapshot) GetMetadata() *SnapshotMetadata {
	if x != nil {
		return x.Metadata
	}
	return nil
}

type Message struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	MsgType  MessageType `protobuf:"varint,1,opt,name=msg_type,json=msgType,proto3,enum=raftproto.MessageType" json:"msg_type,omitempty"`
	To       uint64      `protobuf:"varint,2,opt,name=to,proto3" json:"to,omitempty"`
	From     uint64      `protobuf:"varint,3,opt,name=from,proto3" json:"from,omitempty"`
	Term     uint64      `protobuf:"varint,4,opt,name=term,proto3" json:"term,omitempty"`
	LogTerm  uint64      `protobuf:"varint,5,opt,name=log_term,json=logTerm,proto3" json:"log_term,omitempty"`
	Index    uint64      `protobuf:"varint,6,opt,name=index,proto3" json:"index,omitempty"`
	Entries  []*Entry    `protobuf:"bytes,7,rep,name=entries,proto3" json:"entries,omitempty"`
	Commit   uint64      `protobuf:"varint,8,opt,name=commit,proto3" json:"commit,omitempty"`
	Snapshot *Snapshot   `protobuf:"bytes,9,opt,name=snapshot,proto3" json:"snapshot,omitempty"`
	Reject   bool        `protobuf:"varint,10,opt,name=reject,proto3" json:"reject,omitempty"`
}

func (x *Message) Marshal() (data []byte, err error) {
	//TODO implement me
	panic("implement me")
}

func (x *Message) Reset() {
	*x = Message{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raftpb_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Message) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Message) ProtoMessage() {}

func (x *Message) ProtoReflect() protoreflect.Message {
	mi := &file_raftpb_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Message.ProtoReflect.Descriptor instead.
func (*Message) Descriptor() ([]byte, []int) {
	return file_raftpb_proto_rawDescGZIP(), []int{3}
}

func (x *Message) GetMsgType() MessageType {
	if x != nil {
		return x.MsgType
	}
	return MessageType_MsgHup
}

func (x *Message) GetTo() uint64 {
	if x != nil {
		return x.To
	}
	return 0
}

func (x *Message) GetFrom() uint64 {
	if x != nil {
		return x.From
	}
	return 0
}

func (x *Message) GetTerm() uint64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *Message) GetLogTerm() uint64 {
	if x != nil {
		return x.LogTerm
	}
	return 0
}

func (x *Message) GetIndex() uint64 {
	if x != nil {
		return x.Index
	}
	return 0
}

func (x *Message) GetEntries() []*Entry {
	if x != nil {
		return x.Entries
	}
	return nil
}

func (x *Message) GetCommit() uint64 {
	if x != nil {
		return x.Commit
	}
	return 0
}

func (x *Message) GetSnapshot() *Snapshot {
	if x != nil {
		return x.Snapshot
	}
	return nil
}

func (x *Message) GetReject() bool {
	if x != nil {
		return x.Reject
	}
	return false
}

func (x *Message) Unmarshal(b []byte) (err error) {
	return nil
}

func (x *Message) Size() uint64 {
	return uint64(len(x.Entries))
}

// HardState contains the state of a node need to be peristed, including the current term, commit index
// and the vote record
type HardState struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term   uint64 `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`
	Vote   uint64 `protobuf:"varint,2,opt,name=vote,proto3" json:"vote,omitempty"`
	Commit uint64 `protobuf:"varint,3,opt,name=commit,proto3" json:"commit,omitempty"`
}

func (x *HardState) Reset() {
	*x = HardState{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raftpb_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HardState) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HardState) ProtoMessage() {}

func (x *HardState) ProtoReflect() protoreflect.Message {
	mi := &file_raftpb_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HardState.ProtoReflect.Descriptor instead.
func (*HardState) Descriptor() ([]byte, []int) {
	return file_raftpb_proto_rawDescGZIP(), []int{4}
}

func (x *HardState) GetTerm() uint64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *HardState) GetVote() uint64 {
	if x != nil {
		return x.Vote
	}
	return 0
}

func (x *HardState) GetCommit() uint64 {
	if x != nil {
		return x.Commit
	}
	return 0
}

// ConfState contains the current membership information of the raft group
type ConfState struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// all node id
	Nodes []uint64 `protobuf:"varint,1,rep,packed,name=nodes,proto3" json:"nodes,omitempty"`
}

func (x *ConfState) Reset() {
	*x = ConfState{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raftpb_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ConfState) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ConfState) ProtoMessage() {}

func (x *ConfState) ProtoReflect() protoreflect.Message {
	mi := &file_raftpb_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ConfState.ProtoReflect.Descriptor instead.
func (*ConfState) Descriptor() ([]byte, []int) {
	return file_raftpb_proto_rawDescGZIP(), []int{5}
}

func (x *ConfState) GetNodes() []uint64 {
	if x != nil {
		return x.Nodes
	}
	return nil
}

// ConfChange is the data that attach on entry with EntryConfChange type
type ConfChange struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ChangeType ConfChangeType `protobuf:"varint,1,opt,name=change_type,json=changeType,proto3,enum=raftproto.ConfChangeType" json:"change_type,omitempty"`
	// node will be add/remove
	NodeId  uint64 `protobuf:"varint,2,opt,name=node_id,json=nodeId,proto3" json:"node_id,omitempty"`
	Context []byte `protobuf:"bytes,3,opt,name=context,proto3" json:"context,omitempty"`
}

func (x *ConfChange) Reset() {
	*x = ConfChange{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raftpb_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ConfChange) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ConfChange) ProtoMessage() {}

func (x *ConfChange) ProtoReflect() protoreflect.Message {
	mi := &file_raftpb_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ConfChange.ProtoReflect.Descriptor instead.
func (*ConfChange) Descriptor() ([]byte, []int) {
	return file_raftpb_proto_rawDescGZIP(), []int{6}
}

func (x *ConfChange) GetChangeType() ConfChangeType {
	if x != nil {
		return x.ChangeType
	}
	return ConfChangeType_AddNode
}

func (x *ConfChange) GetNodeId() uint64 {
	if x != nil {
		return x.NodeId
	}
	return 0
}

func (x *ConfChange) GetContext() []byte {
	if x != nil {
		return x.Context
	}
	return nil
}

var File_raftpb_proto protoreflect.FileDescriptor

var file_raftpb_proto_rawDesc = []byte{
	0x0a, 0x0c, 0x72, 0x61, 0x66, 0x74, 0x70, 0x62, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x09,
	0x72, 0x61, 0x66, 0x74, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x7a, 0x0a, 0x05, 0x45, 0x6e, 0x74,
	0x72, 0x79, 0x12, 0x33, 0x0a, 0x0a, 0x65, 0x6e, 0x74, 0x72, 0x79, 0x5f, 0x74, 0x79, 0x70, 0x65,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x14, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x2e, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x54, 0x79, 0x70, 0x65, 0x52, 0x09, 0x65, 0x6e,
	0x74, 0x72, 0x79, 0x54, 0x79, 0x70, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x12, 0x14, 0x0a, 0x05, 0x69,
	0x6e, 0x64, 0x65, 0x78, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x05, 0x69, 0x6e, 0x64, 0x65,
	0x78, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0c, 0x52,
	0x04, 0x64, 0x61, 0x74, 0x61, 0x22, 0x71, 0x0a, 0x10, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f,
	0x74, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x12, 0x33, 0x0a, 0x0a, 0x63, 0x6f, 0x6e,
	0x66, 0x5f, 0x73, 0x74, 0x61, 0x74, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e,
	0x72, 0x61, 0x66, 0x74, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x43, 0x6f, 0x6e, 0x66, 0x53, 0x74,
	0x61, 0x74, 0x65, 0x52, 0x09, 0x63, 0x6f, 0x6e, 0x66, 0x53, 0x74, 0x61, 0x74, 0x65, 0x12, 0x14,
	0x0a, 0x05, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x05, 0x69,
	0x6e, 0x64, 0x65, 0x78, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x04, 0x52, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x22, 0x57, 0x0a, 0x08, 0x53, 0x6e, 0x61, 0x70,
	0x73, 0x68, 0x6f, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x0c, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x12, 0x37, 0x0a, 0x08, 0x6d, 0x65, 0x74, 0x61,
	0x64, 0x61, 0x74, 0x61, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1b, 0x2e, 0x72, 0x61, 0x66,
	0x74, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x4d,
	0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x52, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74,
	0x61, 0x22, 0xb2, 0x02, 0x0a, 0x07, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x31, 0x0a,
	0x08, 0x6d, 0x73, 0x67, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32,
	0x16, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x4d, 0x65, 0x73, 0x73,
	0x61, 0x67, 0x65, 0x54, 0x79, 0x70, 0x65, 0x52, 0x07, 0x6d, 0x73, 0x67, 0x54, 0x79, 0x70, 0x65,
	0x12, 0x0e, 0x0a, 0x02, 0x74, 0x6f, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x02, 0x74, 0x6f,
	0x12, 0x12, 0x0a, 0x04, 0x66, 0x72, 0x6f, 0x6d, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x04,
	0x66, 0x72, 0x6f, 0x6d, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x18, 0x04, 0x20, 0x01,
	0x28, 0x04, 0x52, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x12, 0x19, 0x0a, 0x08, 0x6c, 0x6f, 0x67, 0x5f,
	0x74, 0x65, 0x72, 0x6d, 0x18, 0x05, 0x20, 0x01, 0x28, 0x04, 0x52, 0x07, 0x6c, 0x6f, 0x67, 0x54,
	0x65, 0x72, 0x6d, 0x12, 0x14, 0x0a, 0x05, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x18, 0x06, 0x20, 0x01,
	0x28, 0x04, 0x52, 0x05, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x12, 0x2a, 0x0a, 0x07, 0x65, 0x6e, 0x74,
	0x72, 0x69, 0x65, 0x73, 0x18, 0x07, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x10, 0x2e, 0x72, 0x61, 0x66,
	0x74, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x07, 0x65, 0x6e,
	0x74, 0x72, 0x69, 0x65, 0x73, 0x12, 0x16, 0x0a, 0x06, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x18,
	0x08, 0x20, 0x01, 0x28, 0x04, 0x52, 0x06, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x12, 0x2f, 0x0a,
	0x08, 0x73, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x18, 0x09, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x13, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x53, 0x6e, 0x61, 0x70,
	0x73, 0x68, 0x6f, 0x74, 0x52, 0x08, 0x73, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x12, 0x16,
	0x0a, 0x06, 0x72, 0x65, 0x6a, 0x65, 0x63, 0x74, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x08, 0x52, 0x06,
	0x72, 0x65, 0x6a, 0x65, 0x63, 0x74, 0x22, 0x4b, 0x0a, 0x09, 0x48, 0x61, 0x72, 0x64, 0x53, 0x74,
	0x61, 0x74, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x04, 0x52, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x12, 0x12, 0x0a, 0x04, 0x76, 0x6f, 0x74, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x04, 0x76, 0x6f, 0x74, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x63,
	0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x06, 0x63, 0x6f, 0x6d,
	0x6d, 0x69, 0x74, 0x22, 0x21, 0x0a, 0x09, 0x43, 0x6f, 0x6e, 0x66, 0x53, 0x74, 0x61, 0x74, 0x65,
	0x12, 0x14, 0x0a, 0x05, 0x6e, 0x6f, 0x64, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x04, 0x52,
	0x05, 0x6e, 0x6f, 0x64, 0x65, 0x73, 0x22, 0x7b, 0x0a, 0x0a, 0x43, 0x6f, 0x6e, 0x66, 0x43, 0x68,
	0x61, 0x6e, 0x67, 0x65, 0x12, 0x3a, 0x0a, 0x0b, 0x63, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x5f, 0x74,
	0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x19, 0x2e, 0x72, 0x61, 0x66, 0x74,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x43, 0x6f, 0x6e, 0x66, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65,
	0x54, 0x79, 0x70, 0x65, 0x52, 0x0a, 0x63, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x54, 0x79, 0x70, 0x65,
	0x12, 0x17, 0x0a, 0x07, 0x6e, 0x6f, 0x64, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x04, 0x52, 0x06, 0x6e, 0x6f, 0x64, 0x65, 0x49, 0x64, 0x12, 0x18, 0x0a, 0x07, 0x63, 0x6f, 0x6e,
	0x74, 0x65, 0x78, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x07, 0x63, 0x6f, 0x6e, 0x74,
	0x65, 0x78, 0x74, 0x2a, 0x31, 0x0a, 0x09, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x54, 0x79, 0x70, 0x65,
	0x12, 0x0f, 0x0a, 0x0b, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x4e, 0x6f, 0x72, 0x6d, 0x61, 0x6c, 0x10,
	0x00, 0x12, 0x13, 0x0a, 0x0f, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x43, 0x6f, 0x6e, 0x66, 0x43, 0x68,
	0x61, 0x6e, 0x67, 0x65, 0x10, 0x01, 0x2a, 0xf3, 0x01, 0x0a, 0x0b, 0x4d, 0x65, 0x73, 0x73, 0x61,
	0x67, 0x65, 0x54, 0x79, 0x70, 0x65, 0x12, 0x0a, 0x0a, 0x06, 0x4d, 0x73, 0x67, 0x48, 0x75, 0x70,
	0x10, 0x00, 0x12, 0x0b, 0x0a, 0x07, 0x4d, 0x73, 0x67, 0x42, 0x65, 0x61, 0x74, 0x10, 0x01, 0x12,
	0x0e, 0x0a, 0x0a, 0x4d, 0x73, 0x67, 0x50, 0x72, 0x6f, 0x70, 0x6f, 0x73, 0x65, 0x10, 0x02, 0x12,
	0x0d, 0x0a, 0x09, 0x4d, 0x73, 0x67, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x10, 0x03, 0x12, 0x15,
	0x0a, 0x11, 0x4d, 0x73, 0x67, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x10, 0x04, 0x12, 0x12, 0x0a, 0x0e, 0x4d, 0x73, 0x67, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x56, 0x6f, 0x74, 0x65, 0x10, 0x05, 0x12, 0x1a, 0x0a, 0x16, 0x4d, 0x73, 0x67,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x10, 0x06, 0x12, 0x0f, 0x0a, 0x0b, 0x4d, 0x73, 0x67, 0x53, 0x6e, 0x61, 0x70,
	0x73, 0x68, 0x6f, 0x74, 0x10, 0x07, 0x12, 0x10, 0x0a, 0x0c, 0x4d, 0x73, 0x67, 0x48, 0x65, 0x61,
	0x72, 0x74, 0x62, 0x65, 0x61, 0x74, 0x10, 0x08, 0x12, 0x18, 0x0a, 0x14, 0x4d, 0x73, 0x67, 0x48,
	0x65, 0x61, 0x72, 0x74, 0x62, 0x65, 0x61, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x10, 0x09, 0x12, 0x15, 0x0a, 0x11, 0x4d, 0x73, 0x67, 0x54, 0x72, 0x61, 0x6e, 0x73, 0x66, 0x65,
	0x72, 0x4c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x10, 0x0b, 0x12, 0x11, 0x0a, 0x0d, 0x4d, 0x73, 0x67,
	0x54, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x4e, 0x6f, 0x77, 0x10, 0x0c, 0x2a, 0x2d, 0x0a, 0x0e,
	0x43, 0x6f, 0x6e, 0x66, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x54, 0x79, 0x70, 0x65, 0x12, 0x0b,
	0x0a, 0x07, 0x41, 0x64, 0x64, 0x4e, 0x6f, 0x64, 0x65, 0x10, 0x00, 0x12, 0x0e, 0x0a, 0x0a, 0x52,
	0x65, 0x6d, 0x6f, 0x76, 0x65, 0x4e, 0x6f, 0x64, 0x65, 0x10, 0x01, 0x42, 0x04, 0x5a, 0x02, 0x2e,
	0x2f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_raftpb_proto_rawDescOnce sync.Once
	file_raftpb_proto_rawDescData = file_raftpb_proto_rawDesc
)

func file_raftpb_proto_rawDescGZIP() []byte {
	file_raftpb_proto_rawDescOnce.Do(func() {
		file_raftpb_proto_rawDescData = protoimpl.X.CompressGZIP(file_raftpb_proto_rawDescData)
	})
	return file_raftpb_proto_rawDescData
}

var file_raftpb_proto_enumTypes = make([]protoimpl.EnumInfo, 3)
var file_raftpb_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
var file_raftpb_proto_goTypes = []interface{}{
	(EntryType)(0),           // 0: raftproto.EntryType
	(MessageType)(0),         // 1: raftproto.MessageType
	(ConfChangeType)(0),      // 2: raftproto.ConfChangeType
	(*Entry)(nil),            // 3: raftproto.Entry
	(*SnapshotMetadata)(nil), // 4: raftproto.SnapshotMetadata
	(*Snapshot)(nil),         // 5: raftproto.Snapshot
	(*Message)(nil),          // 6: raftproto.Message
	(*HardState)(nil),        // 7: raftproto.HardState
	(*ConfState)(nil),        // 8: raftproto.ConfState
	(*ConfChange)(nil),       // 9: raftproto.ConfChange
}
var file_raftpb_proto_depIdxs = []int32{
	0, // 0: raftproto.Entry.entry_type:type_name -> raftproto.EntryType
	8, // 1: raftproto.SnapshotMetadata.conf_state:type_name -> raftproto.ConfState
	4, // 2: raftproto.Snapshot.metadata:type_name -> raftproto.SnapshotMetadata
	1, // 3: raftproto.Message.msg_type:type_name -> raftproto.MessageType
	3, // 4: raftproto.Message.entries:type_name -> raftproto.Entry
	5, // 5: raftproto.Message.snapshot:type_name -> raftproto.Snapshot
	2, // 6: raftproto.ConfChange.change_type:type_name -> raftproto.ConfChangeType
	7, // [7:7] is the sub-list for method output_type
	7, // [7:7] is the sub-list for method input_type
	7, // [7:7] is the sub-list for extension type_name
	7, // [7:7] is the sub-list for extension extendee
	0, // [0:7] is the sub-list for field type_name
}

func init() { file_raftpb_proto_init() }
func file_raftpb_proto_init() {
	if File_raftpb_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_raftpb_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Entry); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_raftpb_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SnapshotMetadata); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_raftpb_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Snapshot); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_raftpb_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Message); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_raftpb_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*HardState); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_raftpb_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ConfState); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_raftpb_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ConfChange); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_raftpb_proto_rawDesc,
			NumEnums:      3,
			NumMessages:   7,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_raftpb_proto_goTypes,
		DependencyIndexes: file_raftpb_proto_depIdxs,
		EnumInfos:         file_raftpb_proto_enumTypes,
		MessageInfos:      file_raftpb_proto_msgTypes,
	}.Build()
	File_raftpb_proto = out.File
	file_raftpb_proto_rawDesc = nil
	file_raftpb_proto_goTypes = nil
	file_raftpb_proto_depIdxs = nil
}
