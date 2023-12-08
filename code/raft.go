package code

import "errors"

var (
	ErrProposalDropped = errors.New("raft proposal dropped")
)
