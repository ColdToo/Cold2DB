// Code generated by MockGen. DO NOT EDIT.
// Source: ./raft_node.go

// Package mock is a generated GoMock package.
package mock

import (
	reflect "reflect"

	pb "github.com/ColdToo/Cold2DB/pb"
	raft "github.com/ColdToo/Cold2DB/raft"
	gomock "github.com/golang/mock/gomock"
)

// MockRaftLayer is a mock of RaftLayer interface.
type MockRaftLayer struct {
	ctrl     *gomock.Controller
	recorder *MockRaftLayerMockRecorder
}

// MockRaftLayerMockRecorder is the mock recorder for MockRaftLayer.
type MockRaftLayerMockRecorder struct {
	mock *MockRaftLayer
}

// NewMockRaftLayer creates a new mock instance.
func NewMockRaftLayer(ctrl *gomock.Controller) *MockRaftLayer {
	mock := &MockRaftLayer{ctrl: ctrl}
	mock.recorder = &MockRaftLayerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRaftLayer) EXPECT() *MockRaftLayerMockRecorder {
	return m.recorder
}

// Advance mocks base method.
func (m *MockRaftLayer) Advance() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Advance")
}

// Advance indicates an expected call of Advance.
func (mr *MockRaftLayerMockRecorder) Advance() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Advance", reflect.TypeOf((*MockRaftLayer)(nil).Advance))
}

// ApplyConfChange mocks base method.
func (m *MockRaftLayer) ApplyConfChange(cc pb.ConfChange) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "ApplyConfChange", cc)
}

// ApplyConfChange indicates an expected call of ApplyConfChange.
func (mr *MockRaftLayerMockRecorder) ApplyConfChange(cc interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ApplyConfChange", reflect.TypeOf((*MockRaftLayer)(nil).ApplyConfChange), cc)
}

// GetErrorC mocks base method.
func (m *MockRaftLayer) GetErrorC() chan error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetErrorC")
	ret0, _ := ret[0].(chan error)
	return ret0
}

// GetErrorC indicates an expected call of GetErrorC.
func (mr *MockRaftLayerMockRecorder) GetErrorC() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetErrorC", reflect.TypeOf((*MockRaftLayer)(nil).GetErrorC))
}

// GetReadyC mocks base method.
func (m *MockRaftLayer) GetReadyC() chan raft.Ready {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetReadyC")
	ret0, _ := ret[0].(chan raft.Ready)
	return ret0
}

// GetReadyC indicates an expected call of GetReadyC.
func (mr *MockRaftLayerMockRecorder) GetReadyC() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetReadyC", reflect.TypeOf((*MockRaftLayer)(nil).GetReadyC))
}

// Propose mocks base method.
func (m *MockRaftLayer) Propose(buffer []byte) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Propose", buffer)
	ret0, _ := ret[0].(error)
	return ret0
}

// Propose indicates an expected call of Propose.
func (mr *MockRaftLayerMockRecorder) Propose(buffer interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Propose", reflect.TypeOf((*MockRaftLayer)(nil).Propose), buffer)
}

// ProposeConfChange mocks base method.
func (m *MockRaftLayer) ProposeConfChange(cc pb.ConfChange) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ProposeConfChange", cc)
	ret0, _ := ret[0].(error)
	return ret0
}

// ProposeConfChange indicates an expected call of ProposeConfChange.
func (mr *MockRaftLayerMockRecorder) ProposeConfChange(cc interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ProposeConfChange", reflect.TypeOf((*MockRaftLayer)(nil).ProposeConfChange), cc)
}

// ReportSnapshot mocks base method.
func (m *MockRaftLayer) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "ReportSnapshot", id, status)
}

// ReportSnapshot indicates an expected call of ReportSnapshot.
func (mr *MockRaftLayerMockRecorder) ReportSnapshot(id, status interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReportSnapshot", reflect.TypeOf((*MockRaftLayer)(nil).ReportSnapshot), id, status)
}

// ReportUnreachable mocks base method.
func (m *MockRaftLayer) ReportUnreachable(id uint64) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "ReportUnreachable", id)
}

// ReportUnreachable indicates an expected call of ReportUnreachable.
func (mr *MockRaftLayerMockRecorder) ReportUnreachable(id interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReportUnreachable", reflect.TypeOf((*MockRaftLayer)(nil).ReportUnreachable), id)
}

// Step mocks base method.
func (m_2 *MockRaftLayer) Step(m *pb.Message) error {
	m_2.ctrl.T.Helper()
	ret := m_2.ctrl.Call(m_2, "Step", m)
	ret0, _ := ret[0].(error)
	return ret0
}

// Step indicates an expected call of Step.
func (mr *MockRaftLayerMockRecorder) Step(m interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Step", reflect.TypeOf((*MockRaftLayer)(nil).Step), m)
}

// Stop mocks base method.
func (m *MockRaftLayer) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockRaftLayerMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockRaftLayer)(nil).Stop))
}

// Tick mocks base method.
func (m *MockRaftLayer) Tick() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Tick")
}

// Tick indicates an expected call of Tick.
func (mr *MockRaftLayerMockRecorder) Tick() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Tick", reflect.TypeOf((*MockRaftLayer)(nil).Tick))
}
