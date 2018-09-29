(ns clj-3df.encode
  "Utilties for encoding query plans to something that the backend can
  understand. In particular, attributes and symbols will be encoded as
  integers.")

(def nextID (atom 0))

(def encode-symbol (memoize (fn [sym] #?(:clj  (clojure.lang.RT/nextID)
                                         :cljs (swap! nextID inc)))))

(defn encode-plan [attr->int plan]
  (cond
    (symbol? plan)      (encode-symbol plan)
    (keyword? plan)     (attr->int plan)
    (sequential? plan)  (mapv (partial encode-plan attr->int) plan)
    (associative? plan) (reduce-kv (fn [m k v] (assoc m k (encode-plan attr->int v))) {} plan)
    (nil? plan)         (throw (ex-info "Plan contain's nils."
                                        {:causes #{:contains-nil}}))
    :else               plan))

(defn encode-rules [attr->int rules]
  (mapv (fn [rule]
          (let [{:keys [name plan]} rule]
            {:name name :plan (encode-plan attr->int plan)})) rules))

(comment
  (encode-plan {} [])
  (encode-plan {} '?name)
  (encode-plan {:name 100} '{:HasAttr [?e :name ?n]})
  (encode-plan {:name 200} '{:Join [{:HasAttr [?e1 :name ?n]} {:HasAttr [?e2 :name ?n]} ?n]})
  (encode-plan {:name 300} '{:Join [{:HasAttr [?e1 :name ?n]} {:HasAttr [?e2 :name ?n]} ?n]})
  (encode-plan {:name 400} '{:Project
                             [{:Join [{:HasAttr [?e1 :name ?n]} {:HasAttr [?e2 :name ?n]} ?n]} [?e1 ?n ?e2]]})
  )
