(ns clj-3df.core
  (:refer-clojure :exclude [resolve])
  (:require
   #?(:clj  [clojure.core.async :as async :refer [<! >! >!! <!! go go-loop]]
      :cljs [cljs.core.async :as async :refer [<! >!]])
   #?(:clj  [clojure.spec.alpha :as s]
      :cljs [cljs.spec.alpha :as s])
   #?(:clj  [aleph.http :as http])
   #?(:clj  [manifold.stream :as stream])
   #?(:clj  [cheshire.core :as json])
   #?(:cljs [clj-3df.socket :as socket])
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [clojure.set :as set]
   [clj-3df.compiler :as compiler]
   [clj-3df.encode :as encode])
  #?(:cljs (:require-macros [cljs.core.async.macros :refer [go-loop]]
                            [clj-3df.core :refer [exec!]])))

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
  (-attribute->id [db attr])
  (-id->attribute [db id]))

(defrecord DB [schema rschema attribute->id id->attribute next-tx]
  IDB
  (-schema [db] (.-schema db))
  (-attrs-by [db property] ((.-rschema db) property))
  (-attribute->id [db attr]
    (if-let [[k v] (find attribute->id attr)]
      v
      (throw (ex-info "Unknown attribute." {:attr attr}))))
  (-id->attribute [db id] (id->attribute id)))

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
  (let [attribute->id (into {} (map (juxt identity str) (keys schema)))
        id->attribute (set/map-invert attribute->id)]
    (->DB schema (rschema schema) attribute->id id->attribute 0)))


(defn interest [name]
  [{:Interest {:name name}}])

(defn register-plan [^DB db name plan rules]
  (let [;; @TODO expose this directly?
        ;; the top-level plan is just another rule...
        top-rule {:name name :plan plan}]
    (concat
     [{:Register
       {:publish [name]
        :rules   (->> (conj rules top-rule) (encode/encode-rules (partial -attribute->id db)))}}]
     ;; @TODO split this off
     (interest name))))

(defn register-query
  ([^DB db name query] (register-query db name query []))
  ([^DB db name query rules]
   (let [;; @TODO expose this directly?
         ;; the top-level plan is just another rule...
         top-rule       {:name name :plan (compiler/compile-query query)}
         compiled-rules (if (empty? rules)
                          []
                          (compiler/compile-rules rules))]
     (concat
      [{:Register
        {:publish [name]
         :rules   (->> (conj compiled-rules top-rule)
                       (encode/encode-rules (partial -attribute->id db)))}}]
      ;; @TODO split this off
      (interest name)))))

(defn register-source [name source]
  [{:RegisterSource
    {:name   name
     :source source}}])

(defn create-input [attr]
  [{:CreateInput {:name (str attr)}}])

(defn create-db-inputs [^DB db]
  (reduce-kv (fn [acc k v] (conj acc {:CreateInput {:name (str k)}})) [] (-schema db)))

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
                                    :tx_data
                                    (into tx-data))

                               (sequential? datum)
                               (let [[op e a v] datum]
                                 (conj tx-data [(op->diff op) e (-attribute->id db a) (wrap-type a v)]))))
                           [] tx-data)]
     [{:Transact {:tx tx :tx_data tx-data}}])))

(defrecord Connection [ws out subscriber])

(def xf-parse
  (map (fn [result]
         (let [unwrap-type  (fn [boxed] (second (first boxed)))
               unwrap-tuple (fn [[tuple diff]] [(mapv unwrap-type tuple) diff])
               xf-batch     (map unwrap-tuple)]
           (let [[query_name results] (parse-json result)]
             [query_name (into [] xf-batch results)])))))

#?(:clj (defn create-conn
          ([url]
            (create-conn url nil))
          ([url options]
            (let [ws            @(http/websocket-client url)
                  out           (:channel options (async/chan 100 xf-parse))
                  subscriber    (Thread.
                                 (fn []
                                   (println "[SUBSCRIBER] running")
                                   (loop []
                                     (when-let [result @(stream/take! ws ::drained)]
                                       (if (= result ::drained)
                                         (do
                                           (println "[SUBSCRIBER] server closed connection")
                                           (async/close! out))
                                         (do
                                           (>!! out result)
                                           (recur)))))))]
              (.start subscriber)
              (->Connection ws out subscriber)))))

#?(:cljs (defn create-conn
           ([url]
             (create-conn url nil))
           ([url options]
             (let [ws           (socket/connect url)
                   out          (:channel options (async/chan 100 xf-parse))
                   subscriber   (do
                                  (js/console.log "[SUBSCRIBER] running")
                                  (go-loop []
                                    (when-let [result (<! (:source ws))]
                                      (if (= result :drained)
                                        (do
                                           (println "[SUBSCRIBER] server closed connection")
                                           (async/close! out))
                                        (do
                                          (>! out result)
                                          (recur))))))]
               (->Connection ws out subscriber)))))

(defn debug-conn [url]
  (let [conn (create-conn url)
        out  (:out conn)]
    (go-loop []
      (when-let [msg (<! out)]
        (println msg)
        (recur)))
    conn))

(defn- if-cljs [env then else]
  (if (:ns env) then else))

#?(:clj (defmacro exec! [^Connection conn & forms]
          (if-cljs &env
                   (let [c   (gensym)
                         out (gensym)]
                     `(let [~c   ~conn
                            ~out (.-out ~c)]
                        (cljs.core.async/go
                          (do ~@(for [form forms]
                                (cond
                                  (nil? form) (throw (ex-info "Nil form within execution." {:form form}))
                                  (seq? form)
                                  (case (first form)
                                    'expect-> `(clojure.core/as-> (cljs.core.async/<! ~out) ~@(rest form))
                                    `(clojure.core/->> ~form (clj-3df.core/stringify) (cljs.core.async/>! (:sink (.-ws ~c)))))))))))
                   (let [c   (gensym)
                         out (gensym)]
                     `(let [~c   ~conn
                            ~out (.-out ~c)]
                        (do ~@(for [form forms]
                                (cond
                                  (nil? form) (throw (ex-info "Nil form within execution." {:form form}))
                                  (seq? form)
                                  (case (first form)
                                    'expect-> `(clojure.core/as-> (<!! ~out) ~@(rest form))
                                    `(clojure.core/->> ~form (clj-3df.core/stringify) (stream/put! (.-ws ~c))))))))))))

(comment

  (def conn (create-conn  "ws://127.0.0.1:6262"))

  (def out-pub
    (let [out      (:out conn)
          topic-fn first]
      (async/pub out topic-fn)))

  (defn listener [pub topic-name]
    (let [c (async/chan)
          _ (async/sub pub topic-name c)]
      (go-loop []
               (println (<! c))
               (recur))))

  (def db (create-db {:name {:db/valueType :String} :age {:db/valueType :Number}}))

  (exec! conn (register-query db "basic-conjunction" '[:find ?e ?age :where [?e :name "Mabel"] [?e :age ?age]]))

  (exec! conn (transact db [[:db/add 1 :name "Dipper"] [:db/add 1 :age 30]]))

  (exec! conn (transact db [{:db/id 2 :name "Mabel" :age 26}]))

  (exec! conn (transact db [[:db/retract 2 :name "Mabel"]]))

  (exec! conn (register-query db "basic-disjunction" '[:find ?e :where (or [?e :name "Mabel"] [?e :name "Dipper"])]))

  )


