(ns clj-3df.encode
  "Utilties for encoding query plans to something that the backend can
  understand. In particular, attributes and symbols will be encoded as
  integers.")

(def nextID (atom 0))

(def encode-symbol (memoize (fn [sym] #?(:clj  (clojure.lang.RT/nextID)
                                         :cljs (swap! nextID inc)))))

(defn encode-plan [attribute->id plan]
  (cond
    (symbol? plan)      (encode-symbol plan)
    (keyword? plan)     (attribute->id plan)
    (sequential? plan)  (mapv (partial encode-plan attribute->id) plan)
    (associative? plan) (reduce-kv (fn [m k v] (assoc m k (encode-plan attribute->id v))) {} plan)
    (nil? plan)         (throw (ex-info "Plan contain's nils."
                                        {:causes #{:contains-nil}}))
    :else               plan))

(defn encode-rule [attribute->id rule]
  (let [{:keys [name plan]} rule]
    {:name name
     :plan (encode-plan attribute->id plan)}))

(defn encode-rules [attribute->id rules]
  (mapv (partial encode-rule attribute->id) rules))

(comment
  (encode-plan {} [])
  (encode-plan {} '?name)
  (encode-plan {:name ":name"} '{:MatchA [?e :name ?n]})
  (encode-plan {:name ":name"} '{:Join [?n {:MatchA [?e1 :name ?n]} {:MatchA [?e2 :name ?n]}]})
  (encode-plan {:name ":name"} '{:Join [?n {:MatchA [?e1 :name ?n]} {:MatchA [?e2 :name ?n]}]})
  (encode-plan {:name ":name"} '{:Project
                             [[?e1 ?n ?e2] {:Join [?n {:MatchA [?e1 :name ?n]} {:MatchA [?e2 :name ?n]}]}]})
  )
