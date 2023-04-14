package raft

import "context"

// Node 是算法层中 raft 节点的抽象，也是应用层与算法层交互的唯一入口，应用层持有Node作为算法层raft节点的引用，
// 通过调用 Node 接口的几个 api，完成与算法层的 channel 通信.
type Node interface {
	// Tick ：传送定时驱动信号，每次调用 Tick 方法的时间间隔是固定的，该时间间隔称为一个 tick，是 raft 节点的最小计时单位，
	// 后续 leader 节点的心跳计时和 leader/candidate 的选举计时也都是以 tick 作为时间单位；
	// 实际上，该方法为应用层向 node.tickc channel 中传入一个信号，而算法层的 goroutine 是处于时刻监听该 channel 状态的， 获取到信号后就会驱动 raft 节点进行定时函数处理；
	// 对于 leader 而言，定时函数中的任务是要向集群中的其他节点广播心跳；对于 follower 和 candidate 而言，定时函数的任务是发起竞选；
	// 此外，对于不同角色，在接收到消息时的处理模式也不尽相同，这部分区别会在角色专属的状态机函数中体现.
	Tick()
	// Campaign causes the Node to transition to candidate state and start campaigning to become leader.
	Campaign(ctx context.Context) error

	// Propose 应用层调用 Node.Propose 方法，向算法层发起一笔写数据的请求.
	Propose(ctx context.Context, data []byte) error

	// ProposeConfChange proposes a configuration change. Like any proposal, the
	// configuration change may be dropped with or without an error being
	// returned. In particular, configuration changes are dropped unless the
	// leader has certainty that there is no prior unapplied configuration
	// change in its log.
	//
	// The method accepts either a pb.ConfChange (deprecated) or pb.ConfChangeV2
	// message. The latter allows arbitrary configuration changes via joint
	// consensus, notably including replacing a voter. Passing a ConfChangeV2
	// message is only allowed if all Nodes participating in the cluster run a
	// version of this library aware of the V2 API. See pb.ConfChangeV2 for
	// usage details and semantics.
	// 应用层调用 Propose 方法，最终会往 node.propc channel 中传入一条类型为 MsgProp 的消息.
	// 算法层 goroutine 通过读 channel 获取到消息后，会驱动 raft 节点进入写请求提议流程.
	ProposeConfChange(ctx context.Context, cc pb.ConfChangeI) error

	// Step advances the state machine using the given message. ctx.Err() will be returned, if any.
	// 应用层调用 Node.Step 方法，可以向算法层 goroutine 传送一条自由指定类型的消息，但类型不能是 MsgHup（驱动本节点进行选举）或者
	//MsgBeat（驱动本 leader 节点进行心跳广播），因为这些动作应该是由定时器驱动，而非人为调用.
	Step(ctx context.Context, msg pb.Message) error

	// Ready returns a channel that returns the current point-in-time state.
	// Users of the Node must call Advance after retrieving the state returned by Ready.
	//
	// NOTE: No committed entries from the next Ready may be applied until all committed entries
	// and snapshots from the previous one have finished.
	// 应用层调用 Node.Ready 方法返回 node.readyc channel 用于监听，当应用层从 readyc 中读取到 Ready 信号时，说明算法层已经产生了新一轮的处理结果，应用层需要进行响应处理；
	// 当应用层处理完毕后，需要调用 Node.Advance 方法，通过向 node.advancec channel 中发送信号的方式，示意应用层已处理完毕，算法层可以进入下一轮调度循环.
	// 因此，Node.Ready 和 Node.Advance 方法是成对使用的，当两个方法各被调用一次，意味着应用层与算法层之间完成了一轮交互，紧接着会开启下一轮，周而复始，循环往复.

	Ready() <-chan Ready

	// Advance notifies the Node that the application has saved progress up to the last Ready.
	// It prepares the node to return the next available Ready.
	//
	// The application should generally call Advance after it applies the entries in last Ready.
	//
	// However, as an optimization, the application may call Advance while it is applying the
	// commands. For example. when the last Ready contains a snapshot, the application might take
	// a long time to apply the snapshot data. To continue receiving Ready without blocking raft
	// progress, it can call Advance before finishing applying the last ready.
	Advance()
	// ApplyConfChange applies a config change (previously passed to
	// ProposeConfChange) to the node. This must be called whenever a config
	// change is observed in Ready.CommittedEntries, except when the app decides
	// to reject the configuration change (i.e. treats it as a noop instead), in
	// which case it must not be called.
	//
	// Returns an opaque non-nil ConfState protobuf which must be recorded in
	// snapshots.
	//应用层调用该方法，通过 node.propc hannel 向算法层发送一则消息类型为 MsgProp、日志类型为 EntryConfChange 的消息，
	//推动 raft 节点进入配置变更流程.
	ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState

	// TransferLeadership attempts to transfer leadership to the given transferee.
	TransferLeadership(ctx context.Context, lead, transferee uint64)

	// ReadIndex request a read state. The read state will be set in the ready.
	// Read state has a read index. Once the application advances further than the read
	// index, any linearizable read requests issued before the read request can be
	// processed safely. The read state will have the same rctx attached.
	// Note that request can be lost without notice, therefore it is user's job
	// to ensure read index retries.
	// 应用层调用 node.ReadIndex 方法向算法层发起读数据请求.
	// 实际上会往 node.recvc channel 中传入一条类型为 MsgReadIndex 的消息.
	ReadIndex(ctx context.Context, rctx []byte) error

	// Status returns the current status of the raft state machine.
	Status() Status
	// ReportUnreachable reports the given node is not reachable for the last send.
	ReportUnreachable(id uint64)
	// ReportSnapshot reports the status of the sent snapshot. The id is the raft ID of the follower
	// who is meant to receive the snapshot, and the status is SnapshotFinish or SnapshotFailure.
	// Calling ReportSnapshot with SnapshotFinish is a no-op. But, any failure in applying a
	// snapshot (for e.g., while streaming it from leader to follower), should be reported to the
	// leader with SnapshotFailure. When leader sends a snapshot to a follower, it pauses any raft
	// log probes until the follower can apply the snapshot and advance its state. If the follower
	// can't do that, for e.g., due to a crash, it could end up in a limbo, never getting any
	// updates from the leader. Therefore, it is crucial that the application ensures that any
	// failure in snapshot sending is caught and reported back to the leader; so it can resume raft
	// log probing in the follower.
	ReportSnapshot(id uint64, status SnapshotStatus)
	// Stop performs any necessary termination of the Node.
	Stop()
}
