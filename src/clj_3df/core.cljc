(ns clj-3df.core
  (:refer-clojure :exclude [resolve])
  (:require
   [clojure.string :as str]
   [clojure.set :as set]
   #?(:clj [clojure.spec.alpha :as s]
      :cljs [cljs.spec.alpha :as s])
   [aleph.http :as http]
   [manifold.stream :as stream]
   [cheshire.core :as json]
   [clj-3df.parser :as parser]))

(def ^{:arglists '([db query])} compile-query parser/compile-query)
(def ^{:arglists '([db rules])} plan-rules parser/plan-rules)

(defprotocol IDB
  (-schema [db])
  (-attrs-by [db property])
  (-attr->int [db attr])
  (-int->attr [db i]))

(defrecord Differential [schema rschema attr->int int->attr next-tx impl registrations]
  IDB
  (-schema [db] (.-schema db))
  (-attrs-by [db property] ((.-rschema db) property))
  (-attr->int [db attr] ((.-attr->int db) attr))
  (-int->attr [db i] ((.-int->attr db) i)))

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
    (Differential. schema (rschema schema) attr->int int->attr 0 nil {})))

(defn register-query [db name query]
  (let [compiled (compile-query db query)]
    {:Register {:query_name name
                :plan       (.-plan compiled)
                :in         (.-in compiled)
                :rules      []}}))

(defn register-query! [conn db name query]
  (->> (register-query db name query) (json/generate-string) (stream/put! conn)))

(defn- ^Boolean is-attr? [db attr property] (contains? (-attrs-by db property) attr))
(defn- ^Boolean multival? [db attr] (is-attr? db attr :db.cardinality/many))
(defn- ^Boolean ref? [db attr] (is-attr? db attr :db.type/ref))
(defn- ^Boolean reverse-ref? [attr] (= \_ (nth (name attr) 0)))

(defn- reverse-ref [attr]
  (if (reverse-ref? attr)
    (keyword (namespace attr) (subs (name attr) 1))
    (keyword (namespace attr) (str "_" (name attr)))))

(defn- explode [db entity]
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
          [:db/add v   straight-a eid]
          [:db/add eid straight-a v])))))

(defn transact [db tx-data]
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
                              (map? datum) (into tx-data (transact db (explode db datum)))
                              
                              (sequential? datum)
                              (let [[op e a v] datum]
                                (conj tx-data [(op->diff op) e (-attr->int db a) (wrap-type a v)]))))
                          [] tx-data)]
    {:Transact {:tx_data tx-data}}))

(defn transact! [conn db tx-data]
  (->> (transact db tx-data) (json/generate-string) (stream/put! conn)))

(comment
  (def conn @(http/websocket-client "ws://127.0.0.1:6262"))

  (def subscriber
    (Thread.
     (fn []
       (println "[SUBSCRIBER] running")
       (loop []
         (when-let [result @(stream/take! conn ::drained)]
           (if (= result ::drained)
             (println "[SUBSCRIBER] server closed connection")
             (do
               (println result)
               (recur)))))))))

(comment
  (.start subscriber)
  (.getState subscriber)

  (def schema
    {:name   {:db/valueType :String}
     :age    {:db/valueType :Number}
     :friend {:db/valueType :Eid}
     :edge   {:db/valueType :Eid}
     :admin? {:db/valueType :Bool}})

  (def db (create-db schema))

  (register-query! conn db "test" '[:find ?e :where [?e :name "Mabel"]])

  (transact db [{:db/id  1
                 :name   "Dipper"
                 :age    12
                 :admin? true}
                [:db/add 2 :friend 1]])
  
  (transact! conn db [[:db/add 1 :name "Dipper"]])
  (transact! conn db [[:db/add 2 :name "Mabel"]])
  (transact! conn db [[:db/retract 2 :name "Mabel"]])
  (transact! conn db [[:db/retract 1 :name "Dipper"]
                      [:db/add 1 :name "Mabel"]]))
