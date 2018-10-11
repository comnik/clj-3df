(ns clj-3df.encode
  "Utilties for encoding query plans to something that the backend can
  understand. In particular, attributes and symbols will be encoded as
  integers.")

(def nextID (atom 0))

(def encode-symbol (memoize (fn [sym] #?(:clj  (clojure.lang.RT/nextID)
                                         :cljs (swap! nextID inc)))))

(defn encode-plan [attr->str plan]
  (cond
    (symbol? plan)      (encode-symbol plan)
    (keyword? plan)     (attr->str plan)
    (sequential? plan)  (mapv (partial encode-plan attr->str) plan)
    (associative? plan) (reduce-kv (fn [m k v] (assoc m k (encode-plan attr->str v))) {} plan)
    (nil? plan)         (throw (ex-info "Plan contain's nils."
                                        {:causes #{:contains-nil}}))
    :else               plan))

(defn encode-rule [attr->str rule]
  (let [{:keys [name plan]} rule]
    {:name name
     :plan (encode-plan attr->str plan)}))

(defn encode-rules [attr->str rules]
  (mapv (partial encode-rule attr->str) rules))

(comment
  (encode-plan {} [])
  (encode-plan {} '?name)
  (encode-plan {:name ":name"} '{:MatchA [?e :name ?n]})
  (encode-plan {:name ":name"} '{:Join [?n {:MatchA [?e1 :name ?n]} {:MatchA [?e2 :name ?n]}]})
  (encode-plan {:name ":name"} '{:Join [?n {:MatchA [?e1 :name ?n]} {:MatchA [?e2 :name ?n]}]})
  (encode-plan {:name ":name"} '{:Project
                             [[?e1 ?n ?e2] {:Join [?n {:MatchA [?e1 :name ?n]} {:MatchA [?e2 :name ?n]}]}]})
  )
