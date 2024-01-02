package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/ColdToo/Cold2DB/log"
)

const (
	PUT    = "PUT"
	GET    = "GET"
	DELETE = "DELETE"
	POST   = "POST"
)

type HttpKVAPI struct {
	kvsService *KvService
}

func ServeHttpKVAPI(kvService *KvService, Addr string, doneC <-chan struct{}) {
	srv := http.Server{
		Addr: Addr,
		Handler: &HttpKVAPI{
			kvsService: kvService,
		},
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err.Error())
		}
	}()

	<-doneC
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
		//if v, err = h.kvsService.Lookup(v); err != nil {
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

		ok, err := h.kvsService.Propose([]byte(key), v, false, 0)
		if err != nil {
			return
		}
		if ok {
			w.WriteHeader(http.StatusNoContent)
		}

	case r.Method == DELETE:
		ok, err := h.kvsService.Propose([]byte(key), nil, true, 0)
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
		json.Unmarshal(v, &ConfChangeInfo)

		h.kvsService.ConfChangePropose()
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "Method not allowed,Only support put、get、post、delete", http.StatusMethodNotAllowed)
	}
}
