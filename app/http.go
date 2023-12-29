package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
)

const (
	PUT    = "PUT"
	GET    = "GET"
	DELETE = "DELETE"
	POST   = "POST"
)

type UpdateNodeInfo struct {
	NodeIP string            `json:"node_ip"`
	NodeId uint64            `json:"node_id"`
	NodeOp pb.ConfChangeType `json:"node_op"`
}

type HttpKVAPI struct {
	store       *KvStore
	confChangeC chan<- pb.ConfChange
}

func ServeHttpKVAPI(kvStore *KvStore, Addr string, confChangeC chan<- pb.ConfChange, doneC <-chan struct{}) {
	srv := http.Server{
		Addr: Addr,
		Handler: &HttpKVAPI{
			store:       kvStore,
			confChangeC: confChangeC,
		},
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err.Error())
		}
	}()

	<-doneC
	close(srv.Handler.(*HttpKVAPI).confChangeC)
	if err := srv.Shutdown(nil); err != nil {
		log.Fatal(err.Error())
	}
}

func (h *HttpKVAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.RequestURI
	defer r.Body.Close()

	switch {
	case r.Method == GET:
		//v, err := ioutil.ReadAll(r.Body)
		//if v, err = h.store.Lookup(v); err != nil {
		//	http.Error(w, "Failed to GET", http.StatusNotFound)
		//} else {
		//	w.Write(v)
		//}

	case r.Method == PUT:
		v, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}

		ok, err := h.store.Propose([]byte(key), v, false, 0)
		if err != nil {
			return
		}
		if ok {
			w.WriteHeader(http.StatusNoContent)
		}

	case r.Method == DELETE:
		ok, err := h.store.Propose([]byte(key), nil, true, 0)
		if err != nil {
			return
		}
		if ok {
			w.WriteHeader(http.StatusNoContent)
		}

		//更改节点配置相关
	case r.Method == POST:
		v, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}
		nodeInfo := new(UpdateNodeInfo)
		json.Unmarshal(v, &nodeInfo)

		h.NodeUpdate(nodeInfo, w)

	default:
		http.Error(w, "Method not allowed,Only support put、get、post、delete", http.StatusMethodNotAllowed)
	}
}

func (h *HttpKVAPI) NodeUpdate(updateInfo *UpdateNodeInfo, w http.ResponseWriter) {
	var cc pb.ConfChange
	switch updateInfo.NodeOp {
	case pb.ConfChangeAddNode:
		cc = pb.ConfChange{
			Type:    pb.ConfChangeAddNode,
			NodeID:  updateInfo.NodeId,
			Context: []byte(updateInfo.NodeIP),
		}
	case pb.ConfChangeRemoveNode:
		cc = pb.ConfChange{
			Type:   pb.ConfChangeRemoveNode,
			NodeID: updateInfo.NodeId,
		}
	}
	h.confChangeC <- cc
	// todo 配置变更成功后才应该返回
	w.WriteHeader(http.StatusNoContent)
}
