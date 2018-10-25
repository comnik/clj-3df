(ns clj-3df.compiler-test
  (:require
   [clojure.test :refer [deftest is testing run-tests]]
   [clj-3df.compiler :refer [compile-query compile-rules]]))

;; @TODO
;; Naming guidelines: Tests in here should be named in such a way,
;; that there is a one-to-one correspondence between tests and actual,
;; user-facing query engine features.

(deftest test-patterns
  (is (= '{:MatchEA [876 :name ?n]}
         (compile-query '[:find ?n :where [876 :name ?n]])))

  (is (= '{:MatchE [429 ?a ?v]}
         (compile-query '[:find ?a ?v :where [429 ?a ?v]])))

  (is (= '{:MatchA [?e :name ?v]}
         (compile-query '[:find ?e ?v :where [?e :name ?v]])))

  (is (= '{:MatchAV [?e :name {:String "Dipper"}]}
         (compile-query '[:find ?e :where [?e :name "Dipper"]]))))

(deftest test-find-clause
  (let [query '[:find ?e1 ?n ?e2
                :where [?e1 :name ?n] [?e2 :name ?n]]]

    (testing "error is thrown on unbound symbols in a find-clause"
      (is (thrown? #?(:clj Exception
                      :cljs js/Error)
                   (compile-query '[:find ?unbound :where [?bound :name "Dipper"]]))))

    (is (= '{:Project
             [[?e1 ?n ?e2] {:Join [[?n] {:MatchA [?e1 :name ?n]} {:MatchA [?e2 :name ?n]}]}]}
           (compile-query query)))))

(deftest test-joins
  (testing "simple join"
    (let [query '[:find ?e ?n ?a
                  :where [?e :name ?n] [?e :age ?a]]]
      (is (= '{:Join [[?e] {:MatchA [?e :name ?n]} {:MatchA [?e :age ?a]}]}
             (compile-query query)))))

  (testing "multiple joins"
    (let [query '[:find ?e1 ?n1 ?e2 ?n2
                  :where [?e1 :name ?n1] [?e1 :friend ?e2] [?e2 :name ?n2]]]
      (is (= '{:Project
               [[?e1 ?n1 ?e2 ?n2]
                {:Join
                 [[?e2]
                  {:Join [[?e1] {:MatchA [?e1 :name ?n1]} {:MatchA [?e1 :friend ?e2]}]}
                  {:MatchA [?e2 :name ?n2]}]}]}
             (compile-query query))))))

(deftest test-or
  (testing "simple disjunction"
    (let [query '[:find ?e
                  :where
                  (or [?e :name "Dipper"]
                      [?e :age 12])]]
      (is (= '{:Union
               [[?e] [{:MatchAV [?e :name {:String "Dipper"}]}
                      {:MatchAV [?e :age {:Number 12}]}]]}
             (compile-query query)))))

  (testing "multiple unions on the same symbol should be merged together"
    (let [query '[:find ?e
                  :where (or [?e :name "Dipper"]
                             [?e :age 12]
                             [?e :admin? false])]]
      (is (= '{:Union [[?e]
                       [{:MatchAV [?e :name {:String "Dipper"}]}
                        {:MatchAV [?e :age {:Number 12}]}
                        {:MatchAV [?e :admin? {:Bool false}]}]]}
             (compile-query query))))))

(deftest test-operators
  (testing "simple combination"
    (let [query '[:find ?e
                  :where
                  [?e :name "Mabel"]
                  (or [?e :age 14]
                      [?e :age 12])]]
      (is (= '{:Union
               [[?e]
                [{:Join [[?e]
                         {:MatchAV [?e :name {:String "Mabel"}]}
                         {:MatchAV [?e :age {:Number 14}]}]}
                 {:Join [[?e]
                         {:MatchAV [?e :name {:String "Mabel"}]}
                         {:MatchAV [?e :age {:Number 12}]}]}]]}
             (compile-query query)))))

  (testing "complex combination"
    (let [query '[:find ?e ?salary
                  :where
                  [?e :name "Mabel"]
                  [?e :salary ?salary]
                  (or [?e :age 14]
                      [?e :age 12])]]
      (is (= '{:Union
               [[?e ?salary]
                [{:Join
                  [[?e]
                   {:Join
                    [[?e]
                     {:MatchAV [?e :name {:String "Mabel"}]}
                     {:MatchA [?e :salary ?salary]}]}
                   {:MatchAV [?e :age {:Number 14}]}]}
                 {:Join
                  [[?e]
                   {:Join
                    [[?e]
                     {:MatchAV [?e :name {:String "Mabel"}]}
                     {:MatchA [?e :salary ?salary]}]}
                   {:MatchAV [?e :age {:Number 12}]}]}]]}
             (compile-query query)))))

  (testing "nested combination"
    (let [query '[:find ?e ?salary
                  :where
                  [?e :name "Mabel"]
                  [?e :salary ?salary]
                  (or [?e :age 14]
                      (and [?e :age 12]
                           [?e :salary 1000]))]]
      (is (= '{:Union
               [(?e ?salary)
                [{:Join
                  [[?e]
                   {:Join
                    [[?e]
                     {:MatchAV [?e :name {:String "Mabel"}]}
                     {:MatchA [?e :salary ?salary]}]}
                   {:MatchAV [?e :age {:Number 14}]}]}
                 {:Join
                  [[?e]
                   {:Join
                    [[?e]
                     {:Join
                      [[?e]
                       {:MatchAV [?e :name {:String "Mabel"}]}
                       {:MatchA [?e :salary ?salary]}]}
                     {:MatchAV [?e :age {:Number 12}]}]}
                   {:MatchAV [?e :salary {:Number 1000}]}]}]]}
             (compile-query query))))))

(deftest test-or-join
  (testing "plain union not allowed if any path doesn't bind all symbols"
    (is (thrown? #?(:clj  Exception
                    :cljs js/Error) (compile-query '[:find ?x ?y
                                            :where (or [?x :edge ?y]
                                                       [?x :edge 14]
                                                       (and [?x :edge ?z] [?z :edge ?y]))]))))

  (testing "or-join not allowed if any path doesn't bind all symbols"
    (is (thrown? #?(:clj  Exception
                    :cljs js/Error) (compile-query '[:find ?x ?y ?z
                                            :where (or-join [?x ?y ?z]
                                                     [?x :edge ?y]
                                                     (and [?x :edge ?z] [?z :edge ?y]))]))))

  (testing "or-join hides additional symbols"
    (let [query '[:find ?x ?y
                  :where (or-join [?x ?y]
                           [?x :edge ?y]
                           (and [?x :edge ?z]
                                [?z :edge ?y]))]]
      (is (= '{:Union [[?x ?y]
                       [{:MatchA [?x :edge ?y]}
                        {:Join [[?z] {:MatchA [?x :edge ?z]} {:MatchA [?z :edge ?y]}]}]]}
             (compile-query query))))))

(deftest test-negation
  (testing "simple negation"
    (let [query '[:find ?e
                  :where [?e :age 12] (not [?e :name "Mabel"])]]
      (is (= '{:Antijoin
               [[?e] {:MatchAV [?e :age {:Number 12}]} {:MatchAV [?e :name {:String "Mabel"}]}]}
             (compile-query query)))))

  (testing "fully unbounded not should be rejected"
    (let [query '[:find ?e
                  :where (not [?e :name "Mabel"])]]
      ;; @TODO Decide what is the right semantics here. Disallow or negate?
      (is (thrown? #?(:clj  Exception
                      :cljs js/Error) (compile-query query)))))

  (testing "potential tautologies should be rejected"
    (let [query '[:find ?e
                  :where (or [?e :name "Mabel"]
                             (not [?e :name "Mabel"]))]]
      (is (thrown? #?(:clj  Exception
                      :cljs js/Error) (compile-query query)))))

  (testing "negation is fine, as long as bound by some other clause"
    (let [query '[:find ?e ?name
                  :where
                  [?e :name ?name]
                  (or [?e :name "Mabel"]
                      (not [?e :name "Mabel"]))]]
      ;; @TODO
      #_(is (= {:Join
                [0
                 {:MatchA [0 100 1]}
                 {:Union [[0]
                          {:MatchAV [0 100 {:String "Mabel"}]}
                          {:MatchAV [0 ]}]}]}
               (compile-query query)))))

  (testing "contradiction"
    (let [query '[:find ?e
                  :where (and [?e :name "Mabel"]
                              (not [?e :name "Mabel"]))]]
      (is (= '{:Antijoin
               [[?e]
                {:MatchAV [?e :name {:String "Mabel"}]}
                {:MatchAV [?e :name {:String "Mabel"}]}]}
             (compile-query query))))))

;; @TODO turn into integration test
(deftest test-reachability
  (let [query '[:find ?x ?y
                :where
                (or-join [?x ?y]
                  [?x :edge ?y]
                  (and [?x :edge ?z]
                       (recur ?z ?y)))]]
    (is (= '{:Union [[?x ?y]
                     [{:MatchA [?x :edge ?y]}
                      {:Join [[?z] {:MatchA [?x :edge ?z]} {:RuleExpr [[?z ?y] "recur"]}]}]]}
           (compile-query query)))))

;; @TODO turn into integration test
(deftest test-label-propagation
  (let [query '[:find ?x ?y
                :where
                (or-join [?x ?y]
                  [?x :node ?y]
                  (and [?z :edge ?y]
                       (recur ?x ?z)))]]
    (is (= '{:Union [[?x ?y]
                     [{:MatchA [?x :node ?y]}
                      {:Join [[?z] {:MatchA [?z :edge ?y]} {:RuleExpr [[?x ?z] "recur"]}]}]]}
           (compile-query query)))))

(deftest test-rules
  (testing "simple rule"
    (let [rules '[[(admin? ?user) [?user :admin? true]]]]
      (is (= #{{:name "admin?" :plan '{:MatchAV [?user :admin? {:Bool true}]}}}
             (set (compile-rules rules))))))

  (testing "recursive rule"
    (let [rules '[[(propagate ?x ?y) [?x :node ?y]]
                  [(propagate ?x ?y) [?z :edge ?y] (propagate ?x ?z)]]]
      (is (= #{{:name "propagate"
                :plan '{:Union
                        [[?x ?y]
                         [{:MatchA [?x :node ?y]}
                          {:Join [[?z]
                                  {:MatchA [?z :edge ?y]} {:RuleExpr [[?x ?z] "propagate"]}]}]]}}}
             (set (compile-rules rules))))))

  (testing "rules can be split across many bodies"
    (let [rules '[;; @TODO check whether datomic allows this
                  ;; [(subtype ?t1 ?t2) [?t2 :name "Object"]]
                  [(subtype ?t1 ?t2) [?t1 :name ?any] [?t2 :name "Object"]]
                  [(subtype ?t1 ?t2) [?t1 :name ?n] [?t2 :name ?n]]
                  [(subtype ?t1 ?t2) [?t1 :extends ?t2]]
                  [(subtype ?t1 ?t2) [?t1 :extends ?any] (subtype ?any ?t2)]]]
      (is (= #{{:name "subtype"
                :plan {:Union
                       [[1 0]
                        [{:Filter [0 100 {:String "Object"}]}
                         {:Join [[2] {:MatchA [1 100 2]} {:MatchA [0 100 2]}]}
                         {:MatchA [1 700 0]}
                         {:Join
                          [[3] {:MatchA [1 700 3]} {:RuleExpr [[3 0] "subtype"]}]}]]}}}
             (set (compile-rules rules)))))))

(deftest test-predicates
  (testing "simple, top-level predicate"
    (let [query '[:find ?a1 ?a2
                  :where
                  [?user :age ?a1]
                  [?user :age ?a2]
                  [(< ?a1 ?a2)]]]
      (is (= '{:Project
               [[?a1 ?a2]
                {:Filter
                 [[?a1 ?a2]
                  "LT"
                  {:Join [[?user] {:MatchA [?user :age ?a1]} {:MatchA [?user :age ?a2]}]} {}]}]}
             (compile-query query)))))

  (testing "predicate union"
    (let [query '[:find ?user ?age
                  :where
                  [?user :age ?age]
                  (or [(>= ?age 21)]
                      [(< ?age 18)])]]
      (is (= '{:Union
               [[?user ?age]
                [{:Filter [[?age] "GTE" {:MatchA [?user :age ?age]} {1 {:Number 21}}]}
                 {:Filter [[?age] "LT" {:MatchA [?user :age ?age]} {1 {:Number 18}}]}]]}
             (compile-query query)))))

  (testing "nested predicates"
    (let [query '[:find ?child1 ?child2
                  :where
                  [?parent :child ?child1] [?child1 :timestamp ?t1]
                  [?parent :child ?child2] [?child2 :timestamp ?t2]
                  (or [(> ?t1 ?t2)]
                      (and [(= ?parent ?child1)]
                           [(< ?t1 ?t2)]))]]
      (is (= '{:Union
               [(?child1 ?child2)
                [{:Filter
                  [[?t1 ?t2]
                   "GT"
                   {:Join
                    [[?child2]
                     {:Join
                      [[?parent]
                       {:Join
                        [[?child1]
                         {:MatchA [?parent :child ?child1]}
                         {:MatchA [?child1 :timestamp ?t1]}]}
                       {:MatchA [?parent :child ?child2]}]}
                     {:MatchA [?child2 :timestamp ?t2]}]} {}]}
                 {:Filter
                  [[?parent ?child1]
                   "EQ"
                   {:Filter
                    [[?t1 ?t2]
                     "LT"
                     {:Join
                      [[?child2]
                       {:Join
                        [[?parent]
                         {:Join
                          [[?child1]
                           {:MatchA [?parent :child ?child1]}
                           {:MatchA [?child1 :timestamp ?t1]}]}
                         {:MatchA [?parent :child ?child2]}]}
                       {:MatchA [?child2 :timestamp ?t2]}]} {}]} {}]}]]}
             (compile-query query)))))

  #_(testing "nested predicates w/ cartesian"
      (let [query '[:find ?child1 ?child2
                    :where
                    [?child1 :id/ctr ?ctr1] [?child1 :id/node ?n1]
                    [?child2 :id/ctr ?ctr2] [?child2 :id/node ?n2]
                    (or [(> ?ctr1 ?ctr2)]
                        (and [(= ?ctr1 ?ctr2)]
                             [(> ?n1 ?n2)]))]]
        (is (= '{:Union
                 [[?child1 ?child2]
                  {:Filter [[?ctr1 ?ctr2] "GT"
                              {:Join [{:MatchA [?child1 :id/ctr ?ctr1]}
                                      {:MatchA [?child1 :id/ctr ?ctr1]}]} {}]}
                  {:Filter [[?age] "LT" {:MatchA [?user :age ?age]}]}]}
             (compile-query query)))))

  (testing "nested predicates + patterns"
    (let [query '[:find ?child1 ?child2
                  :where
                  [?child1 :id/ctr ?ctr1] [?child1 :id/node ?n1]
                  [?child2 :id/ctr ?ctr2] [?child2 :id/node ?n2]
                  (or [(> ?ctr1 ?ctr2)]
                      (and [?child1 :foo "bar"]
                           [(= ?ctr1 ?ctr2)]
                           [(> ?n1 ?n2)]))]]
      (is (= '{:Union
               [(?child1 ?child2)
                [{:Filter
                  [[?ctr1 ?ctr2]
                   "GT"
                   {:Join
                    [[]
                     {:Join
                      [[?child2]
                       {:MatchA [?child2 :id/ctr ?ctr2]}
                       {:MatchA [?child2 :id/node ?n2]}]}
                     {:Join
                      [[?child1]
                       {:MatchA [?child1 :id/ctr ?ctr1]}
                       {:MatchA [?child1 :id/node ?n1]}]}]} {}]}
                 {:Filter
                  [[?ctr1 ?ctr2]
                   "EQ"
                   {:Filter
                    [[?n1 ?n2]
                     "GT"
                     {:Join
                      [[]
                       {:Join
                        [[?child1]
                         {:Join
                          [[?child1]
                           {:MatchA [?child1 :id/ctr ?ctr1]}
                           {:MatchA [?child1 :id/node ?n1]}]}
                         {:MatchAV [?child1 :foo {:String "bar"}]}]}
                       {:Join
                        [[?child2]
                         {:MatchA [?child2 :id/ctr ?ctr2]}
                         {:MatchA [?child2 :id/node ?n2]}]}]} {}]} {}]}]]}
             (compile-query query))))))

;; (deftest test-inputs
;;   (let [query '[:find ?user ?age
;;                 :in ?max-age
;;                 :where
;;                 [?user :age ?age]
;;                 [(< ?age ?max-age)]]]
;;     (is (= '{:Filter
;;              ["LT" [?age ?max-age]
;;               {:Join
;;                [{:MatchA [?user :age ?age]}
;;                 {:Input [?max-age]}]}]}
;;            (compile-query query)))))

;; @TODO turn into integration test
(deftest test-lww
  (let [rules '[[(older? ?t1 ?key)
                 [?op :assign/key ?key] [?op :assign/time ?t1]
                 [?op2 :assign/key ?key] [?op2 :assign/time ?t2]
                 [(< ?t1 ?t2)]]

                [(lww ?key ?val)
                 [?op :assign/time ?t]
                 [?op :assign/key ?key]
                 [?op :assign/value ?val]
                 (not (older? ?t ?key))]]]
    (is (= #{{:name "older?"
              :plan '{:Project
                      [[?t1 ?key]
                       {:Filter
                        [[?t1 ?t2]
                         "LT"
                         {:Join
                          [[?op2]
                           {:Join
                            [[?key]
                             {:Join
                              [[?op]
                               {:MatchA [?op :assign/key ?key]}
                               {:MatchA [?op :assign/time ?t1]}]}
                             {:MatchA [?op2 :assign/key ?key]}]}
                           {:MatchA [?op2 :assign/time ?t2]}]} {}]}]}}
             {:name "lww"
              :plan '{:Project
                      [[?key ?val]
                       {:Antijoin
                        [[?t ?key]
                         {:Join
                          [[?op]
                           {:Join
                            [[?op]
                             {:MatchA [?op :assign/time ?t]}
                             {:MatchA [?op :assign/key ?key]}]}
                           {:MatchA [?op :assign/value ?val]}]}
                         {:RuleExpr [[?t ?key] "older?"]}]}]}}}
           (set (compile-rules rules))))))

(deftest test-aggregations
  (testing "min"
    (let [query '[:find ?user (min ?age)
                  :where [?user :age ?age]]]
      (is (= '{:Aggregate [[?user ?age] {:MatchA [?user :age ?age]} "MIN" [?user]]}
             (compile-query query))))))

(deftest test-functions
  (testing "fn"
    (let [query '[:find ?e ?t
                  :where
                  [?e :event/time ?t] [(interval ?t) ?t]]]
      (is (= '{:Transform
               [[?t]
                {:MatchA [?e :event/time ?t]}
                "INTERVAL"]}
             (compile-query query)))))
  (testing "fn-pred"
    (let [query '[:find ?e ?t
                  :in ?cutoff
                  :where
                  [?e :event/time ?t][(> ?t ?cutoff)][(interval ?t) ?t]]]
      (is (= '{:Transform
               [[?t]
                {:Filter [[?t ?cutoff] "GT" {:MatchA [?e :event/time ?t]} {}]}
                "INTERVAL"]}
             (compile-query query))))))
