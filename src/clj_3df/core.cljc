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
  (-attr->int [db attr])
  (-int->attr [db i]))

(defrecord DB [schema rschema attr->int int->attr next-tx]
  IDB
  (-schema [db] (.-schema db))
  (-attrs-by [db property] ((.-rschema db) property))
  (-attr->int [db attr]
    (if-let [[k v] (find attr->int attr)]
      v
      (throw (ex-info "Unknown attribute." {:attr attr}))))
  (-int->attr [db i] (int->attr i)))

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
  (let [attr->int (zipmap (keys schema) (iterate (partial + 100) 100))
        int->attr (set/map-invert attr->int)]
    (->DB schema (rschema schema) attr->int int->attr 0)))

(defn register-plan [^DB db name plan rules-plan]
  {:Register {:query_name name
              :plan       (encode/encode-plan (partial -attr->int db) plan)
              :rules      (encode/encode-rules (partial -attr->int db) rules-plan)}})

(defn register-query
  ([^DB db name query] (register-query db name query []))
  ([^DB db name query rules]
   (let [rules-plan (if (empty? rules)
                      []
                      (compiler/compile-rules rules))]
     {:Register {:query_name name
                 :plan       (encode/encode-plan (partial -attr->int db) (compiler/compile-query query))
                 :rules      (encode/encode-rules (partial -attr->int db) rules-plan)}})))

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
                                    :Transact
                                    :tx_data
                                    (into tx-data))
                               
                               (sequential? datum)
                               (let [[op e a v] datum]
                                 (conj tx-data [(op->diff op) e (-attr->int db a) (wrap-type a v)]))))
                           [] tx-data)]
     {:Transact {:tx tx :tx_data tx-data}})))

(defrecord Connection [ws out subscriber])

#?(:clj (defn create-conn [url]
          (let [ws            @(http/websocket-client url)
                out-chan     (async/chan 50)
                out          (async/pub  out-chan :topic)
                unwrap-type  (fn [boxed] (second (first boxed)))
                unwrap-tuple (fn [[tuple diff]] [(mapv unwrap-type tuple) diff])
                xf-batch     (map unwrap-tuple)
                subscriber   (Thread.
                              (fn []
                                (println "[SUBSCRIBER] running")
                                (loop []
                                  (when-let [result @(stream/take! ws ::drained)]
                                    (if (= result ::drained)
                                      (println "[SUBSCRIBER] server closed connection")
                                      (let [[query_name results] (parse-json result)]
                                        (>!! out-chan  {:topic :out :value [query_name (into [] xf-batch results)]})
                                        (recur)))))))]
            (.start subscriber)
            (->Connection ws out subscriber))))

#?(:cljs (defn create-conn [url]
           (let [ws           (socket/connect url {:source (async/chan 50) :sink (async/chan 50)})
                 out-chan     (async/chan 50)
                 out          (async/pub  out-chan :topic)
                 unwrap-type  (fn [boxed] (second (first boxed)))
                 unwrap-tuple (fn [[tuple diff]] [(mapv unwrap-type tuple) diff])
                 xf-batch     (map unwrap-tuple)
                 subscriber   (do
                                (js/console.log "[SUBSCRIBER] running")
                                (go-loop []
                                  (when-let [result (<! (:source ws))]
                                    (if (= result :drained)
                                      (js/console.log "[SUBSCRIBER] server closed connection")
                                      (let [[query_name results] (parse-json result)]
                                        (>! out-chan  {:topic :out :value [query_name (into [] xf-batch results)]})
                                        (recur))))))]
             (->Connection ws out subscriber))))

(defn debug-conn [url]
  (let [conn (create-conn url)
        sub-chan (async/chan 50)
        _  (async/sub (:out conn) :out sub-chan)]
    (go-loop []
      (when-let [{:keys [value]} (<! sub-chan)]
        (println value))
      (recur))
    conn))

(defn- if-cljs [env then else]
  (if (:ns env) then else))

#?(:clj (defmacro exec! [^Connection conn & forms]
          (if-cljs &env
                   (let [c   (gensym)
                         out (gensym)
                         _   (gensym)]
                     `(let [~c ~conn
                            ~out  (cljs.core.async/chan 50)
                            ~_    (cljs.core.async/sub (.-out ~c) :out ~out)]
                        (cljs.core.async/go
                          (do ~@(for [form forms]
                                (cond
                                  (nil? form) (throw (ex-info "Nil form within execution." {:form form}))
                                  (seq? form)
                                  (case (first form)
                                    'expect-> `(clojure.core/as-> (:value (cljs.core.async/<! ~out)) ~@(rest form))
                                    `(clojure.core/->> ~form (clj-3df.core/stringify) (cljs.core.async/>! (:sink (.-ws ~c)))))))))))
                   (let [c   (gensym)
                         out (gensym)
                         _   (gensym)]
                     `(let [~c ~conn
                            ~out  (async/chan 50)
                            ~_    (async/sub (.-out ~c) :out ~out)]
                        (do ~@(for [form forms]
                                (cond
                                  (nil? form) (throw (ex-info "Nil form within execution." {:form form}))
                                  (seq? form)
                                  (case (first form)
                                    'expect-> `(clojure.core/as-> (:value (<!! ~out)) ~@(rest form))
                                    `(clojure.core/->> ~form (clj-3df.core/stringify) (stream/put! (.-ws ~c))))))))))))
