package main

import (
	"fmt"
	"log"
	"net/http"
)


func health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
	  w.Header().Set("Allow", http.MethodGet)
	  http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	  return
}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprint(w, "ccb")
}

func main(){
	http.HandleFunc("/health", health)
	log.Println("准备监听http://localhost:8081")

	err := http.ListenAndServe("localhost:8081", nil)
	if err != nil {
		log.Fatal(err)
	}
}

