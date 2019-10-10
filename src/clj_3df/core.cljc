(ns clj-3df.core
  (:refer-clojure :exclude [resolve])
  (:require
   #?(:clj  [clojure.spec.alpha :as s]
      :cljs [cljs.spec.alpha :as s])
   #?(:clj  [aleph.http :as http])
   #?(:clj  [manifold.stream :as stream])
   #?(:clj  [cheshire.core :as json])
   #?(:cljs [clj-3df.socket :as socket])
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [clojure.set :as set]
   [clojure.walk :refer [keywordize-keys]]
   [clj-3df.compiler :as compiler]
   [clj-3df.attribute :refer [of-type input-semantics tx-time]]
   [clj-3df.encode :as encode]))

;; HELPER

(defn parse-json [json]
  #?(:clj  (json/parse-string json)
     :cljs (js->clj (.parse js/JSON json))))

(defn stringify [obj]
  #?(:clj (cheshire.core/generate-string obj)
     :cljs (.stringify js/JSON (clj->js obj))))

(defprotocol IDB
  (-schema [db])
  (-attrs-by [db property])
  (-id->attribute [db id]))

(defrecord DB [schema rschema next-tx]
  IDB
  (-schema [db] (.-schema db))
  (-attrs-by [db property] ((.-rschema db) property)))

(defn- ^Boolean is-attr? [^DB db attr property] (contains? (-attrs-by db property) attr))
(defn- ^Boolean multival? [^DB db attr] (is-attr? db attr :db.cardinality/many))
(defn- ^Boolean ref? [^DB db attr] (is-attr? db attr :db.type/ref))
(defn- ^Boolean reverse-ref? [attr] (= \_ (nth (name attr) 0)))

(defn attr->properties [k v]
  (case v
    :db.unique/identity  [:db/unique :db.unique/identity :db/index]
    :db.unique/value     [:db/unique :db.unique/value :db/index]
    :db.cardinality/many [:db.cardinality/many]
    :db.type/ref         [:db.type/ref :db/index]
    :db.type/derived     [:db.type/derived]
    (when (true? v)
      (case k
        :db/isComponent [:db/isComponent]
        :db/index       [:db/index]
        []))))

(defn- rschema [schema]
  (reduce-kv
    (fn [m attr keys->values]
      (reduce-kv
        (fn [m key value]
          (reduce
            (fn [m prop]
              (assoc m prop (conj (get m prop #{}) attr)))
            m (attr->properties key value)))
        m keys->values))
    {} schema))

(defn create-db [schema]
  (->DB schema (rschema schema) 0))

(defn interest [name]
  [{:Interest {:name name}}])

(defn uninterest [name]
  [{:Uninterest name}])

(defn register [^DB db name plan rules]
  (let [;; @TODO expose this directly?
        ;; the top-level plan is just another rule...
        top-rule {:name name :plan plan}]
    [{:Register
      {:publish [name]
       :rules   (encode/encode-rules (conj rules top-rule))}}]))

(defn register-query
  ([^DB db name query] (register-query db name query []))
  ([^DB db name query rules]
   (let [;; @TODO expose this directly?
         ;; the top-level plan is just another rule...
         top-rule       {:name name :plan (compiler/compile-query query)}
         compiled-rules (if (empty? rules)
                          []
                          (compiler/compile-rules rules))]
     [{:Register
       {:publish [name]
        :rules   (encode/encode-rules (conj compiled-rules top-rule))}}])))

(defn query
  ([^DB db name q] (query db name q []))
  ([^DB db name q rules]
   (concat
    (register-query db name q rules)
    (interest name))))

(defn register-source [source]
  [{:RegisterSource source}])

(defn create-attribute
  "Creates a CreateAttribute command with the provided
  configuration. The various ways to configure attributes are defined
  in the `clj-3df.attribute` namespace."
  [attr config]
  [{:CreateAttribute
    {:name   (encode/encode-keyword attr)
     :config (merge (select-keys config [:input_semantics :trace_slack :index_direction :query_support])
                    {:timeless true})}}])

(defn create-db-inputs [^DB db]
  (->> (seq (.-schema db))
       (mapcat (fn [[name config]]
                 (create-attribute name config)))))

(defn advance-domain
  "Creates an AdvanceDomain command to the specified next timestamp. The
  timestamp must match the time domain of the server."
  [next-t]
  [{:AdvanceDomain [nil next-t]}])

(defn close-input [attr]
  [{:CloseInput (encode/encode-keyword attr)}])

(defn- reverse-ref [attr]
  (if (reverse-ref? attr)
    (keyword (namespace attr) (subs (name attr) 1))
    (keyword (namespace attr) (str "_" (name attr)))))

(defn- explode [^DB db entity]
  (let [eid (:db/id entity)]
    (for [[a vs] entity
          :when  (not= a :db/id)
          :let   [reverse?   (reverse-ref? a)
                  straight-a (if reverse? (reverse-ref a) a)
                  _          (when (and reverse? (not (ref? db straight-a)))
                               (throw
                                (ex-info "Reverse attribute name requires {:db/valueType :db.type/ref} in schema."
                                         {:error :transact/syntax, :attribute a, :context {:db/id eid, a vs}})))]
          v      (if (multival? db a) vs [vs])]
      (if (and (ref? db straight-a) (map? v)) ;; another entity specified as nested map
        (assoc v (reverse-ref a) eid)
        (if reverse?
          [:db/add v straight-a eid]
          [:db/add eid straight-a v])))))

(defn transact
  ([^DB db tx-data] (transact db nil tx-data))
  ([^DB db tx tx-data]
   (let [schema    (-schema db)
         op->diff  (fn [op]
                     (case op
                       :db/add     1
                       :db/retract -1))
         wrap-type (fn [a v]
                     (let [type (get-in schema [a :db/valueType] :db.type/unknown)]
                       (if (= type :db.type/unknown)
                         (throw (ex-info "Unknown value type" {:type type}))
                         {type v})))
         tx-data   (reduce (fn [tx-data datum]
                             (cond
                               (map? datum)
                               (->> (explode db datum)
                                    (transact db tx)
                                    first
                                    :Transact
                                    (into tx-data))

                               (sequential? datum)
                               (let [[op e a v t] datum]
                                 (conj tx-data [(op->diff op) (encode/encode-value e) (encode/encode-keyword a) (wrap-type a v) t]))))
                           [] tx-data)]
     [{:Transact tx-data}])))

(defmulti parse-output
  (fn [x] (first (keys x))))

(defmethod parse-output "Error" [result]
  (let [[client error tx-id] (get result "Error")]
    ["Error" (merge {"df.client" client} error {"df.tx-id" tx-id})]))

(defmethod parse-output "Message" [result]
  (let [[client value] (get result "Message")]
    ["Message" (merge {"df.client" client} value)]))

(defmethod parse-output "Json" [result]
  (let [[query-name value time diff] (get result "Json")]
     [query-name value time diff]))

(defmethod parse-output "QueryDiff" [result]
  (let [[query-name results] (get result "QueryDiff")
        keywordize           (fn [v] (map keywordize-keys v))
        result               (->> results
                                  (map keywordize))]
    [query-name result]))

(defn parse-result
  [result]
  (parse-output (parse-json result)))

(defrecord Connection [ws listeners query-listeners])

(defn listen!
  "Registers a callback that gets called on any message received from
  the 3DF server."
  ([^Connection conn callback] (listen! conn (rand) callback))
  ([^Connection conn key callback]
   (swap! (.-listeners conn) assoc key callback)
   key))

(defn unlisten!
  "Unregisters a callback that was previously registered via `listen!`."
  [^Connection conn key]
  (swap! (.-listeners conn) dissoc key))

(defn listen-query!
  "Registers a callback that gets called on any result diffs received
  for the specified query."
  ([^Connection conn query callback] (listen-query! conn query (rand) callback))
  ([^Connection conn query key callback]
   (swap! (.-query-listeners conn) assoc-in [query key] callback)
   key))

(defn unlisten-query!
  "Unregisters a callback that was previously registered via
  `listen-query!`."
  [^Connection conn query key]
  (swap! (.-query-listeners conn) update query dissoc key))

#?(:clj (defn create-conn!
          [url]
          (let [conn (->Connection nil (atom {}) (atom {}))
                ws   @(http/websocket-client url)
                mw   (Thread.
                      (fn []
                        (println "[MIDDLEWARE] running")
                        (loop []
                          (when-let [result @(stream/take! ws ::drained)]
                            (if (= result ::drained)
                              (println "[MIDDLEWARE] server closed connection")
                              (let [data (parse-result result)]
                                (let [listeners @(.-listeners conn)]
                                  (doseq [listener (vals listeners)]
                                    (apply listener [data])))
                                (let [[query diff]    data
                                      query-listeners @(.-query-listeners conn)
                                      listeners       (get query-listeners query [])]
                                  (doseq [listener (vals listeners)]
                                    (apply listener [diff])))
                                (recur)))))))]
            (.start mw)
            (assoc conn :ws ws))))

#?(:cljs (defn create-conn!
           [url]
           (let [conn       (->Connection nil (atom {}) (atom {}))
                 on-open    (fn [_] (println "Socket opened"))
                 on-message (fn [e]
                              (let [data (parse-result (.-data e))]
                                (let [listeners @(.-listeners conn)]
                                  (doseq [listener (vals listeners)]
                                    (apply listener [data])))
                                (let [[query diff]    data
                                      query-listeners @(.-query-listeners conn)
                                      listeners       (get query-listeners query [])]
                                  (doseq [listener (vals listeners)]
                                    (apply listener [diff])))))
                 on-close   (fn [_] (println "Socket closed"))]
             (assoc conn
                    :ws (socket/connect! url on-open on-message on-close)))))

(defn create-debug-conn!
  "Shortcut to create a new connection that will pretty-print all
  received messages."
  [url]
  (let [conn (create-conn! url)]
    (listen! conn pprint/pprint)
    conn))

(defn exec!
  "Batches and serializes the provided requests and sends them along the
  3DF connection."
  [^Connection conn & batches]
  (->> batches
       (apply concat)
       (stringify)
       #?(:clj (stream/put! (.-ws conn)))
       #?(:cljs (socket/put! (.-ws conn)))))



(comment

  (def conn (create-conn! "ws://127.0.0.1:6262"))
  (def conn (create-debug-conn! "ws://127.0.0.1:6262"))

  (def schema
    {:loan/amount  (merge
                    (of-type :Number)
                    (input-semantics :db.semantics.cardinality/many)
                    (tx-time))
     :loan/from    (merge
                    (of-type :String)
                    (input-semantics :db.semantics.cardinality/many)
                    (tx-time))
     :loan/to      (merge
                    (of-type :String)
                    (input-semantics :db.semantics.cardinality/many)
                    (tx-time))
     :loan/over-50 (merge
                    (of-type :Bool)
                    (input-semantics :db.semantics.cardinality/many)
                    (tx-time))})
  
  (def db (create-db schema))

  (exec! conn (create-db-inputs db))

  (def loans
    "an overview of all loans in the system"
    '[:find ?loan ?from ?amount ?to ?over-50
      :where
      [?loan :loan/amount ?amount]
      [?loan :loan/from ?from]
      [?loan :loan/to ?to]
      [?loan :loan/over-50 ?over-50]])
  
  (exec! conn
    (query db "loans" loans))

  (exec! conn
    (transact db [[:db/add 1 :loan/amount 100]
                  [:db/add 1 :loan/from "A"]
                  [:db/add 1 :loan/to "B"]
                  [:db/add 1 :loan/over-50 false]]))

  (exec! conn
    (query db "loans>50"
     '[:find ?loan ?amount
       :where
       [?loan :loan/amount ?amount]
       [(> ?amount 50)]]))

  (listen-query!
   conn "loans>50"
   (fn [diffs]
     (println "executing rule loans>50" diffs)
     (doseq [[[id amount] t op] diffs]
       (when (pos? op)
         (exec! conn (transact db [[:db/retract (encode/decode-value id) :loan/over-50 false]]))
         (exec! conn (transact db [[:db/add (encode/decode-value id) :loan/over-50 (> (encode/decode-value amount) 50)]]))))))

  (exec! conn
    (transact db [{:db/id        2
                   :loan/amount  200
                   :loan/from    "B"
                   :loan/to      "A"
                   :loan/over-50 false}]))

  (exec! conn
    (transact db [[:db/retract 1 :loan/amount 100]]))
  )

