(ns clj-3df.compiler
  (:require
   [clojure.string :as str]
   [clojure.set :as set]
   #?(:clj [clojure.spec.alpha :as s]
      :cljs [cljs.spec.alpha :as s])))

;; UTIL

(def nextID (atom 0))

(def ^:dynamic debug? false)
(def log-level :info)

(defmacro info [& args]
  `(println ~@args))

(defmacro trace [& args]
  (when (= log-level :trace)
    `(println ~@args)))

(defmacro trace-bindings [msg bindings]
  (when (= log-level :trace)
    `(do (println ~msg)
         (clojure.pprint/pprint (mapv plan ~bindings)))))

(defn- pipe-log
  ([ctx] (pipe-log ctx "log:"))
  ([ctx message]
   (info message ctx)
   ctx))

(def ^{:arglists '([pred] [pred coll])} separate (juxt filter remove))

;; GRAMMAR

(s/def ::query (s/keys :req-un [::find ::where]
                       :opt-un [::in]))

(s/def ::find (s/alt ::find-rel ::find-rel))
(s/def ::find-rel (s/+ ::find-elem))
(s/def ::find-elem (s/or :var ::variable :aggregate ::aggregate))

(s/def ::in (s/+ ::variable))

(s/def ::where (s/+ ::clause))

(s/def ::clause
  (s/or ::and (s/cat :marker #{'and} :clauses (s/+ ::clause))
        ::or (s/cat :marker #{'or} :clauses (s/+ ::clause))
        ::or-join (s/cat :marker #{'or-join} :symbols (s/and vector? (s/+ ::variable)) :clauses (s/+ ::clause))
        ::not (s/cat :marker #{'not} :clauses (s/+ ::clause))
        ::not-join (s/cat :marker #{'not-join} :symbols (s/and vector? (s/+ ::variable)) :clauses (s/+ ::clause))
        ::pred-expr (s/tuple (s/cat :predicate ::predicate :fn-args (s/+ ::fn-arg)))
        ::lookup (s/tuple ::eid keyword? ::variable)
        ::entity (s/tuple ::eid ::variable ::variable)
        ::hasattr (s/tuple ::variable keyword? ::variable)
        ::filter (s/tuple ::variable keyword? ::value)
        ::rule-expr (s/cat :rule-name ::rule-name :fn-args (s/+ ::fn-arg))))

(s/def ::aggregate (s/cat :aggregation-fn ::aggregation-fn :vars (s/+ ::variable)))

(s/def ::rules (s/and vector? (s/+ ::rule)))
(s/def ::rule (s/and vector?
                     (s/+ (s/cat :head ::rule-head
                                 :clauses (s/+ ::clause)))))
(s/def ::rule-head (s/and list? (s/cat :name ::rule-name :vars (s/+ ::variable))))
(s/def ::rule-name (s/and symbol? #(-> % name (str/starts-with? "?") (not))))

(s/def ::eid number?)
;; (s/def ::symbol (s/or ::placeholder #{'_}
;;                       ::variable ::variable))
(s/def ::variable (s/and symbol?
                         #(-> % name (str/starts-with? "?"))))
(s/def ::value (s/or :number number?
                     :string string?
                     :bool   boolean?))
(s/def ::predicate '#{<= < > >= = not=})
(s/def ::aggregation-fn '#{min max count})
(s/def ::fn-arg (s/or :var ::variable :const ::value))

;; PARSING

(defn query->map [query]
  (loop [parsed {}, key nil, qs query]
    (if-let [q (first qs)]
      (if (keyword? q)
        (recur parsed q (next qs))
        (recur (update-in parsed [key] (fnil conj []) q) key (next qs)))
      parsed)))

(defn parse-query [query]
  (let [query     (if (sequential? query) (query->map query) query)
        conformed (s/conform ::query query)]
    (if (s/invalid? conformed)
      (throw (ex-info "Couldn't parse query" (s/explain-data ::query query)))
      conformed)))

(defn parse-rules [rules]
  (let [conformed (s/conform ::rules rules)]
    (if (s/invalid? conformed)
      (throw (ex-info "Couldn't parse rules" (s/explain-data ::rules rules)))
      conformed)))

;; QUERY PLAN GENERATION

;; When resolving queries we have a single goal: determining a unique
;; binding for every logic variable. Therefore we need to specify how
;; to unify two competing bindings in all possible contexts
;; (e.g. 'and', 'or', 'not').

(defprotocol IBinding
  (bound-symbols [binding] "Returns the ordered sequence of symbols bound by this binding.")
  (conflict? [binding other] "Returns true iff the 'other' binding conflicts with this binding.")
  (plan [binding] "Returns a query plan to compute this binding."))

(defn- debug-plan [b]
  (binding [debug? true] (plan b)))

(defn- binds?
  "Returns true iff the binding binds the specified symbol."
  [binding symbol]
  (some #{symbol} (bound-symbols binding)))

(defn- binds-all?
  "Returns true iff the binding binds all of the specified symbols."
  [binding symbols]
  (every? #(binds? binding %) symbols))

(defn- shared-symbols
  "Returns the set of symbols that is bound by both bindings."
  [& bindings]
  (apply set/intersection (map (comp set bound-symbols) bindings)))

(defn- extract-binding
  "Extracts the final remaining binding from a context of unified
  bindings. Will throw if more than one binding is still present."
  [ctx]
  (cond
    (empty? ctx)      (throw (ex-info "No binding available." {:ctx (debug-plan ctx)}))
    (> (count ctx) 1) (throw (ex-info "More than one binding present." {:ctx (mapv debug-plan ctx)}))
    :else             (first ctx)))

(defrecord Disjunction [conjunctions symbols]
  IBinding
  (bound-symbols [this]
    (if (some? (seq symbols)) symbols (bound-symbols (first (first conjunctions)))))
  (plan [this]
    (let [symbols      (bound-symbols this)
          conjunctions (map extract-binding conjunctions)]
      (if (every? #(binds-all? % symbols) conjunctions)
        {:Union [symbols (mapv plan conjunctions)]}
        (if debug?
          {:Union [symbols (mapv plan conjunctions)]}
          (throw (ex-info "Bindings must be union compatible inside of an or-clause. Insert suitable projections."
                          {:unified (debug-plan this)})))))))

(defrecord Negation [conjunction symbols]
  IBinding
  (bound-symbols [this]
    (if (some? (seq symbols)) symbols (bound-symbols (first conjunction))))
  (plan [this]
    (if debug?
      {:Negation [:_]}
      (throw (ex-info "Unbound negation." {:negation this})))))

;; Some clauses (inputs, data patterns and rule expressions) generate
;; bindings by themselves, we call them relations.

(defrecord Input [symbol value]
  IBinding
  (bound-symbols [this] [(.-symbol this)])
  (plan [this] {:Input (bound-symbols this)}))

(defrecord Relation [type pattern symbols]
  IBinding
  (bound-symbols [rel] (.-symbols rel))
  (plan [{:keys [type pattern]}]
    (let [encode-value (fn [v]
                         (cond
                           (string? v)  {:String v}
                           (number? v)  {:Number v}
                           (boolean? v) {:Bool v}))]
      (case type
        ::lookup  (let [[e a sym-v] pattern] {:Lookup [e a sym-v]})
        ::entity  (let [[e sym-a sym-v] pattern] {:Entity [e sym-a sym-v]})
        ::hasattr (let [[sym-e a sym-v] pattern] {:HasAttr [sym-e a sym-v]})
        ::filter  (let [[sym-e a [_ v]] pattern] {:Filter [sym-e a (encode-value v)]})))))

(defrecord RuleExpr [rule-name symbols]
  IBinding
  (bound-symbols [this] symbols)
  (plan [this] {:RuleExpr [(str rule-name) (bound-symbols this)]}))

;; Predicate epxressions, aggregations, and projections act on
;; existing bindings.

(defrecord Predicate [predicate args binding]
  IBinding
  (bound-symbols [this]
    (if (some? binding) (bound-symbols binding) args))
  (plan [this]
    (let [encode-predicate {'< "LT" '<= "LTE" '> "GT" '>= "GTE" '= "EQ" 'not= "NEQ"}
          symbols          (bound-symbols this)]
      (if (some? binding)
        {:PredExpr [(encode-predicate predicate) args (plan binding)]}
        (if debug?
          {:PredExpr [(encode-predicate predicate) args :_]}
          (throw (ex-info "All predicate inputs must be bound in a single relation." {:binding this})))))))

(defrecord Aggregation [fn-symbol args binding]
  IBinding
  (bound-symbols [this]
    (if (some? binding) (bound-symbols binding) args))
  (plan [this]
    (if (some? binding)
      {:Aggregate [(str/upper-case (name fn-symbol)) (plan binding) args]}
      (if debug?
        {:Aggregate [(str/upper-case (name fn-symbol)) :_ args]}
        (throw (ex-info "All aggregate arguments must be bound by a single relation." {:binding this}))))))

(defrecord Projection [binding symbols]
  IBinding
  (bound-symbols [this] symbols)
  (plan [this]
    (let [symbols (bound-symbols this)]
      (cond
        (= symbols (bound-symbols binding)) (plan binding)
        (binds-all? binding symbols)        (cond
                                              ;; Union does a projection anyways.
                                              (instance? Disjunction binding) (plan (assoc binding :symbols symbols))
                                              :else                           {:Project [(plan binding) symbols]})
        debug?                              {:Project [(plan binding) symbols]}
        :else                               (throw (ex-info "Projection on unbound symbols." {:binding (debug-plan this)}))))))

;; In the first pass the tree of (potentially) nested,
;; context-modifying operators is navigated and all bindings are
;; brought into a form that we can work with. This mostly means
;; creating the right binding representation and transforming
;; constants into input bindings.

(defn- const->in
  "Transforms any constant arguments into inputs."
  [args]
  (->> args
       (reduce
        (fn [state [typ arg]]
          (case typ
            :var   (update state :normalized-args conj arg)
            :const (let [in (gensym "?in_")]
                     (-> state
                         (update :inputs conj (->Input in [:const arg]))
                         (update :normalized-args conj in)))))
        {:inputs [] :normalized-args []})))

(defmulti normalize (fn [ctx clause] (first clause)))

(defmethod normalize ::and [ctx [_ {:keys [clauses]}]]
  (into ctx (reduce normalize [] clauses)))

(defmethod normalize ::or [ctx [_ {:keys [clauses]}]]
  (let [conjunctions (map #(normalize [] %) clauses)]
    (conj ctx (->Disjunction conjunctions []))))

(defmethod normalize ::or-join [ctx [_ {:keys [symbols clauses]}]]
  (let [conjunctions (map #(normalize [] %) clauses)]
    (conj ctx (->Disjunction conjunctions symbols))))

(defmethod normalize ::not [ctx [_ {:keys [clauses]}]]
  (conj ctx (->Negation (reduce normalize [] clauses) [])))

(defmethod normalize ::not-join [ctx [_ {:keys [symbols clauses]}]]
  (conj ctx (->Negation (reduce normalize [] clauses) symbols)))

(defmethod normalize ::pred-expr [ctx [_ predicate-expr]]
  (let [[{:keys [predicate fn-args]}]    predicate-expr
        {:keys [inputs normalized-args]} (const->in fn-args)]
    (-> ctx
        (into inputs)
        (conj (->Predicate predicate normalized-args nil)))))

(defmethod normalize ::lookup [ctx [_ [e a sym-v :as pattern]]]
  (conj ctx (->Relation ::lookup pattern [sym-v])))

(defmethod normalize ::entity [ctx [_ [e sym-a sym-v :as pattern]]]
  (conj ctx (->Relation ::entity pattern [sym-a sym-v])))

(defmethod normalize ::hasattr [ctx [_ [sym-e a sym-v :as pattern]]]
  (conj ctx (->Relation ::hasattr pattern [sym-e sym-v])))

(defmethod normalize ::filter [ctx [_ [sym-e a v :as pattern]]]
  (conj ctx (->Relation ::filter pattern [sym-e])))

(defmethod normalize ::rule-expr [ctx [_ rule-expr]]
  (let [{:keys [rule-name fn-args]}      rule-expr
        {:keys [inputs normalized-args]} (const->in fn-args)]
    (-> ctx
        (into inputs)
        (conj (->RuleExpr rule-name normalized-args)))))

(comment
  (->> '[:find ?e ?n :where [?e :name ?n]] parse-query :where (reduce normalize []))

  (->> '[:find ?e
         :where
         (or-join [?e]
           [?e :name "Dipper"]
           (and [?e :friend ?e2]
                [?e2 :name "Dipper"]))] parse-query :where (reduce normalize []))
  
  (->> '[:find ?e ?n ?a
         :where
         [?e :name ?n] [?e :age ?a]
         (or [(< ?a 10)]
             [(> ?a 18)])] parse-query :where (reduce normalize []))

  (->> '[:find ?e ?n ?a
         :where
         (and [?e :name ?n] [?e :age ?a])
         (or [(< ?a 10)]
             [(> ?a 18)])] parse-query :where (reduce normalize []))

  (->> '[:find ?e ?n ?a
         :where
         [?e :name ?n] [?e :age ?a]
         (or [(< ?a 10)]
             (and [(> ?a 18)]
                  [?e :admin? true]))] parse-query :where (reduce normalize []))

  (->> '[:find ?e :where [?e :name ?name] (not [?e :name "Dipper"])] parse-query :where (reduce normalize []))

  (->> '[:find ?e ?n ?a
         :where
         [?e :name ?n] [?e :age ?a]
         (not [(< ?a 10)]
              [(> ?a 18)]
              [?e :admin? true])] parse-query :where (reduce normalize []))
  )

;; FIND SPEC

(defn- extract-find-symbols [[typ pattern]]
  (case typ
    ::find-rel (mapcat extract-find-symbols pattern)
    :var       [pattern]
    :aggregate (:vars pattern)))

(defn- extract-aggregations [[typ pattern]]
  (case typ
    ::find-rel (mapcat extract-aggregations pattern)
    :var       []
    :aggregate [pattern]))

(comment
  (-> '[:find ?e ?n :where [?e :name ?n]] parse-query :find extract-find-symbols)

  (-> '[:find ?e (count ?n) :where [?e :name ?n]] parse-query :find extract-find-symbols)

  (-> '[:find ?e (count ?n) :where [?e :name ?n]] parse-query :find extract-aggregations)
  )

;; UNIFICATION

;; We process the set of bindings until all conflicts are
;; resolved. Clauses are in conflict iff they share one or more
;; symbols. ific (i.e. nested) context they share. Conflicting
;; bindings are resolved via unification. Context determines the
;; method of unification (conjunction / disjunction). Conflicts must
;; be resolved within the most specific (i.e. deeply nested) contexts
;; first, before continuing up the hierarchy (which means that
;; introducing clauses one-by-one doesn't fully cut it). If a symbol
;; is bound in two different contexts, the enclosing one determines
;; the method of unification.

;; @TODO join more than two bindings
(defrecord Join [bindings]
  IBinding
  (bound-symbols [this]
    (let [shared    (apply shared-symbols bindings)
          join-syms (into [] shared)]
      (reduce concat join-syms (map #(remove shared (bound-symbols %)) bindings))))
  (plan [this]
    (let [symbols (bound-symbols this)]
      {:Join [(plan (first (.-bindings this))) (plan (second (.-bindings this))) (into [] (apply shared-symbols bindings))]})))

;; @TODO antijoin more than two bindings
(defrecord Antijoin [bindings]
  IBinding
  (bound-symbols [this]
    (let [shared      (apply shared-symbols bindings)
          join-syms   (into [] shared)]
      (concat join-syms (remove shared (bound-symbols (first bindings))))))
  (plan [this]
    {:Antijoin [(plan (first bindings)) (plan (second bindings)) (into [] (apply shared-symbols bindings))]}))

;; Define conflicts and how to resolve them.

(derive Input ::binding)
(derive Relation ::binding)
(derive RuleExpr ::binding)
(derive Predicate ::binding)
(derive Join ::binding)
(derive Antijoin ::binding)

(defn conflicting?
  "Returns true iff two bindings are in conflict."
  [b1 b2]
  (some? (seq (shared-symbols b1 b2))))

(defmulti unify
  "Resolves two conflicting bindings."
  (fn [b1 b2] [(type b1) (type b2)]))

(defn- unify-with
  "Adds a new binding into a collection of existing, unified bindings
  and resolves any conflicts thus introduced."
  [unified binding]
  (trace-bindings "INTRODUCE" [binding])
  (let [[conflicts free] (separate #(conflicting? binding %) unified)]
    (if (empty? conflicts)
      (conj unified binding)
      (let [_ (trace-bindings "CONFLICT" conflicts)
            resolved (reduce (fn [binding other]
                               (trace "unify" (debug-plan other) (debug-plan binding))
                               (unify other binding)) binding conflicts)]
        (unify-with free resolved)))))

(comment
  (unify-with []
              (->Relation ::hasattr '[?e :name ?n] '[?e ?n]))

  (unify-with [(->Relation ::hasattr '[?e :name ?n] '[?e ?n])]
              (->Relation ::hasattr '[?e :age ?age] '[?e ?age]))

  (unify-with [(->Relation ::hasattr '[?e :age ?age] '[?e ?age])]
              (->Predicate '> '[?age] nil))

  (unify-with [(->Relation ::hasattr '[?e :age ?age] '[?e ?age])]
              (->Negation [(->Relation ::filter '[?e :age [:number 18]] '[?e])] []))
  )

(defn unify-conjunction
  "Unifies a conjunctive context w.r.t. to a collection of existing,
  unified bindings (usually handed down from an enclosing context)."
  [unified bindings]
  (reduce unify-with unified bindings))

(defmethod unify :default [b1 b2]
  (cond
    (nil? b1) b2
    (nil? b2) b1
    :else
    (case [(:negated? b1 false) (:negated? b2 false)]
      [false false] (->Join [b1 b2])
      [false true]  (->Antijoin [b1 b2])
      [true false]  (->Antijoin [b2 b1]))))

;; (defmethod unify [Input Predicate] [b1 b2 op] (unify b2 b1 op))
;; (defmethod unify [Predicate Input] [pred inp op]
;;   (if-some [binding (.-binding pred)]
;;     (update pred :binding unify inp op)
;;     (update pred :symbols #(->> %
;;                                 (into [] (remove (set (bound-symbols inp))))))))

;; @TODO handle case where more than one binding remains in conjunction (cartesian?)
(defmethod unify [Negation ::binding] [b1 b2] (->Antijoin [b2 (first (:conjunction b1))]))
(defmethod unify [::binding Negation] [b1 b2] (->Antijoin [b1 (first (:conjunction b2))]))
(defmethod unify [Negation Negation] [b1 b2] (update b1 :conjunction unify-conjunction (:conjunction b2)))

(defmethod unify [::binding Predicate] [b1 b2] (unify b2 b1))
(defmethod unify [Predicate ::binding] [pred binding] (update pred :binding unify binding))
(defmethod unify [Predicate Projection] [b1 b2] (unify b2 b1))
(defmethod unify [Projection Predicate] [proj pred] (update proj :binding unify pred))
;; @TODO compose predicate functions
(defmethod unify [Predicate Predicate] [p1 p2] (update p1 :binding unify p2))

(defn unify-context
  "Unifies a full context, which can contain both conjunctions and
  disjunctions, w.r.t. to a collection of existing, unified
  bindings (usually handed down from an enclosing context)."
  ([ctx] (unify-context [] ctx))
  ([unified ctx]
   (reduce (fn [unified next]
             (cond
               (instance? Disjunction next)
               (let [unified    (with-meta unified {:duplication-tag #?(:clj  (clojure.lang.RT/nextID)
                                                                        :cljs (swap! nextID inc))})
                     unify-path (partial unify-context unified)]
                 [(-> next
                      ;; P1 AND (P2 OR P3) = (P1 AND P2) OR (P1 AND P3)
                      (update :conjunctions #(map unify-path %))
                      ;; make sure or-join doesn't shadow anything
                      (update :symbols into (mapcat bound-symbols unified))
                      (update :symbols distinct))])

               :else
               (unify-with unified next))) unified ctx)))

(comment
  (unify-context [] [(->Relation ::hasattr '[?e :name ?n] '[?e ?n])])

  (unify-context [] [(->Relation ::hasattr '[?e :name ?n] '[?e ?n])
                     (->Relation ::hasattr '[?e :age ?age] '[?e ?age])])

  (unify-context [] [(->Relation ::hasattr '[?e :name ?n] '[?e ?n])
                     (->Disjunction [[(->Relation ::filter '[?e :age [:number 18]] '[?e])]
                                     [(->Relation ::filter '[?e :age [:number 22]] '[?e])]] [])])

  (unify-context [] [(->Relation ::hasattr '[?e :age ?age] '[?e ?age])
                     (->Predicate '> '[?age] nil)])

  (unify-context [] [(->Relation ::hasattr '[?e :age ?age] '[?e ?age])
                     (->Negation [(->Relation ::filter '[?e :age [:number 18]] '[?e])] [])])
  )

;; PUTTING IT ALL TOGETHER

(defn compile-query [query]
  (let [ir          (parse-query query)
        unified     (->> (:where ir) (reduce normalize []) unify-context extract-binding)
        projection  (->> (:find ir) extract-find-symbols (->Projection unified))
        aggregation (->> (:find ir)
                         extract-aggregations
                         (reduce (fn [unified {:keys [aggregation-fn vars]}]
                                   (->Aggregation aggregation-fn vars unified)) projection))]
    (plan aggregation)))

(comment
  (compile-query '[:find ?e ?n :where [?e :name ?n]])

  (compile-query '[:find ?unbound :where [?bound :name "Dipper"]])

  (compile-query '[:find ?n ?e :where [?e :name ?n]])

  (compile-query '[:find ?e :where [?e :name "Test"]])

  (compile-query '[:find ?e ?n ?a :where [?e :age ?a] [?e :name ?n]])

  (compile-query '[:find ?e ?a
                   :where
                   [?e :age ?a]
                   (or [?e :name "Mabel"] [?e :name "Dipper"])])

  (compile-query '[:find ?e ?a
                   :where
                   [?e :age ?a]
                   (or-join [?e]
                     [?e :name "Dipper"]
                     (and [?e :friend ?e2]
                          [?e2 :name "Dipper"]))])

  (compile-query '[:find ?user (min ?age) :where [?user :age ?age]])

  (compile-query '[:find ?key ?t1 ?t2
                   :where
                   [?op1 :timestamp ?t1] [?op1 :key ?key]
                   [?op2 :timestamp ?t2] [?op2 :key ?key]
                   [(< ?t1 ?t2)]])

  (compile-query '[:find ?e :where (not [?e :name "Mabel"])])

  (compile-query '[:find ?e ?name
                   :where [?e :name ?name] (not [?e :name "Mabel"])])

  (compile-query '[:find (count ?artist)
                   :where
                   [?artist :artist/name ?name]
                   (not-join [?artist]
                     [?release :release/artists ?artist]
                     [?release :release/year 1970])])
  
  (compile-query '[:find ?e ?n ?a
                   :where
                   [?e :name ?n] [?e :age ?a]
                   (or [(< ?a 10)]
                       [(> ?a 18)])])
  )

(defn compile-rules [rules]
  (let [ir       (parse-rules rules)
        get-head #(get-in % [0 :head])
        by-head  (group-by get-head ir)

        ;; We perform a transformation here, wrapping body clauses
        ;; with (and) and rule definitions with (or). Note that :Union
        ;; with a single relation is equivalent to a :Project.
        get-clauses  #(get-in % [0 :clauses])
        wrap-and     (fn [clauses] [::and {:clauses clauses}])
        rewrite-rule (fn [rewritten head rules]
                       (let [wrapped-clauses (map (comp wrap-and get-clauses) rules)]
                         (if (= (count rules) 1)
                           (assoc rewritten head (get-clauses (first rules)))
                           (assoc rewritten head [[::or-join {:symbols (:vars head)
                                                              :clauses wrapped-clauses}]]))))
        rewritten    (reduce-kv rewrite-rule {} by-head)
        
        compile-rewritten (fn [compiled head where-clause]
                            (let [unified    (->> where-clause (reduce normalize []) unify-context extract-binding)
                                  ;; @TODO projection to rule head
                                  projection (->> (:vars head) (->Projection unified))]
                              (assoc compiled head projection)))
        compiled          (reduce-kv compile-rewritten {} rewritten)

        rel->rule (fn [rules head binding]
                    (conj rules {:name (str (:name head))
                                 :plan (plan binding)}))
        rules     (reduce-kv rel->rule #{} compiled)]
    (into [] rules)))

(comment
  (compile-rules '[[(admin? ?user) [?user :admin? true]]])

  (compile-rules '[[(propagate ?x ?y) [?x :node ?y]]
                   [(propagate ?x ?y) [?z :edge ?y] (propagate ?x ?z)]])

  (let [q0 '[:find ?t1 ?t2
             :where
             [?op :time ?t1]
             [?op :time ?t2]
             [(< ?t2 100)]
             (or [(< ?t1 ?t2)]
                 [(< ?t2 ?t1)]
                 (and [(= ?t1 ?t2)]
                      (not [?op2 :time ?t2])
                      (some-rule ?t1 ?t2)
                      (another-rule ?op "ADD")))]
        q1 '[:find ?op ?op2
             :where
             [?op :time ?t]
             [?op2 :time ?t]]
        q2 '[:find ?t1 ?t2
             :where
             [?op :time ?t1]
             [?op2 :time ?t2]
             (yarule ?t1 ?t2)
             [(< ?t1 ?t2)]]]
    (compile-query q0))
  )

