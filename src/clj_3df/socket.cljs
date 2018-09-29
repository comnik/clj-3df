(ns clj-3df.socket
  "A namespace for opening WebSockets in ClojureScript.
   Thanks to https://github.com/weavejester/haslett."
  (:require [cljs.core.async :as async :refer [<! >!]])
  (:require-macros [cljs.core.async.macros :refer [go-loop]]))

(defn connect
  "Create a WebSocket to the specified URL, and returns a 'stream' map of three keys:
   - source: async chan to listen to.
   - sink: async chan to write to
   - socket: the WebSocket
   In options one can supply custom channels and protocols."
  ([url]
   (connect url {}))
  ([url options]
   (let [protocols (into-array (:protocols options []))
         socket    (js/WebSocket. url protocols)
         source    (:source options (async/chan))
         sink      (:sink   options (async/chan))
         stream    {:socket socket :source source :sink sink}]
     (set! (.-binaryType socket) (name (:binary-type options :arraybuffer)))
     (set! (.-onopen socket)     (fn [_] (go-loop []
                                           (when-let [msg (<! sink)]
                                             (.send socket msg)
                                             (recur)))))
     (set! (.-onmessage socket)  (fn [e] (async/put! source (.-data e))))
     (set! (.-onclose socket)    (fn [e]
                                   (async/put! source :drained)
                                   (async/close! source)
                                   (async/close! sink)))
     stream)))

(defn close
  "Close a stream opened by connect."
  [stream]
  (.close (:socket stream))
  (:close-status stream))
