// Command webhook-receiver is a tiny local endpoint for alert webhooks. It
// verifies the X-Obs-Signature HMAC (when WEBHOOK_SECRET is set) and
// prints every notification as one JSON line.
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9095", "listen address")
	flag.Parse()
	secret := []byte(os.Getenv("WEBHOOK_SECRET"))
	http.HandleFunc("POST /alerts", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "read error", http.StatusBadRequest)
			return
		}
		verified := "unsigned"
		if len(secret) > 0 {
			m := hmac.New(sha256.New, secret)
			m.Write(body)
			if hmac.Equal([]byte(r.Header.Get("X-Obs-Signature")), []byte("sha256="+hex.EncodeToString(m.Sum(nil)))) {
				verified = "valid"
			} else {
				verified = "INVALID"
				http.Error(w, "bad signature", http.StatusUnauthorized)
			}
		}
		var payload any
		json.Unmarshal(body, &payload)
		line, _ := json.Marshal(map[string]any{"received_at": time.Now().UTC(), "signature": verified, "notification": payload})
		fmt.Println(string(line))
	})
	log.Printf("webhook receiver listening on http://%s/alerts", *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
