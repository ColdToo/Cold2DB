package main

import (
	"encoding/json"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"io/ioutil"
	"net/http"
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
		<-doneC
		if err := srv.Shutdown(nil); err != nil {
			log.Fatal(err.Error())
		}
	}()
}

func (h *HttpKVAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.RequestURI
	defer r.Body.Close()

	switch {
	case r.Method == GET:
		if v, err := h.store.Lookup([]byte(key)); err != nil {
			w.Write(v)
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}

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
		//todo 支持批量delete
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
		w.Header().Set("Allow", "PUT")
		w.Header().Add("Allow", "GET")
		w.Header().Add("Allow", "POST")
		w.Header().Add("Allow", "DELETE")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
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
	w.WriteHeader(http.StatusNoContent)
}
