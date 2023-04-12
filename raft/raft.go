package raft

type RoleType uint8

const (
	Leader RoleType = iota
	Follower
	Candidate
)
