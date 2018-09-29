(ns clj-3df.compiler-test
  (:require
   [clojure.test :refer [deftest is testing run-tests]]
   [clj-3df.compiler :refer [compile-query compile-rules]]))

;; @TODO
;; Naming guidelines: Tests in here should be named in such a way,
;; that there is a one-to-one correspondence between tests and actual,
;; user-facing query engine features.

(deftest test-patterns  
  (is (= '{:Lookup [876 :name ?n]}
         (compile-query '[:find ?n :where [876 :name ?n]])))

  (is (= '{:Entity [429 ?a ?v]}
         (compile-query '[:find ?a ?v :where [429 ?a ?v]])))

  (is (= '{:HasAttr [?e :name ?v]}
         (compile-query '[:find ?e ?v :where [?e :name ?v]])))

  (is (= '{:Filter [?e :name {:String "Dipper"}]}
         (compile-query '[:find ?e :where [?e :name "Dipper"]]))))

(deftest test-find-clause
  (let [query '[:find ?e1 ?n ?e2
                :where [?e1 :name ?n] [?e2 :name ?n]]]

    (testing "error is thrown on unbound symbols in a find-clause"
      (is (thrown? #?(:clj Exception
                      :cljs js/Error)
                   (compile-query '[:find ?unbound :where [?bound :name "Dipper"]]))))

    (is (= '{:Project
             [{:Join [{:HasAttr [?e1 :name ?n]} {:HasAttr [?e2 :name ?n]} [?n]]} [?e1 ?n ?e2]]}
           (compile-query query)))))

(deftest test-joins
  (testing "simple join"
    (let [query '[:find ?e ?n ?a
                  :where [?e :name ?n] [?e :age ?a]]]
      (is (= '{:Join [{:HasAttr [?e :name ?n]} {:HasAttr [?e :age ?a]} [?e]]}
             (compile-query query)))))

  (testing "multiple joins"
    (let [query '[:find ?e1 ?n1 ?e2 ?n2
                  :where [?e1 :name ?n1] [?e1 :friend ?e2] [?e2 :name ?n2]]]
      (is (= '{:Project
               [{:Join
                 [{:Join [{:HasAttr [?e1 :name ?n1]} {:HasAttr [?e1 :friend ?e2]} [?e1]]}
                  {:HasAttr [?e2 :name ?n2]}
                  [?e2]]}
                [?e1 ?n1 ?e2 ?n2]]}
             (compile-query query))))))

(deftest test-or
  (testing "simple disjunction"
    (let [query '[:find ?e
                  :where
                  (or [?e :name "Dipper"]
                      [?e :age 12])]]
      (is (= '{:Union
               [[?e] [{:Filter [?e :name {:String "Dipper"}]}
                      {:Filter [?e :age {:Number 12}]}]]}
             (compile-query query)))))

  (testing "multiple unions on the same symbol should be merged together"
    (let [query '[:find ?e
                  :where (or [?e :name "Dipper"]
                             [?e :age 12]
                             [?e :admin? false])]]
      (is (= '{:Union [[?e]
                       [{:Filter [?e :name {:String "Dipper"}]}
                        {:Filter [?e :age {:Number 12}]}
                        {:Filter [?e :admin? {:Bool false}]}]]}
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
                [{:Join [{:Filter [?e :name {:String "Mabel"}]}
                         {:Filter [?e :age {:Number 14}]}
                         [?e]]}
                 {:Join [{:Filter [?e :name {:String "Mabel"}]}
                         {:Filter [?e :age {:Number 12}]}
                         [?e]]}]]}
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
                  [{:Join
                    [{:Filter [?e :name {:String "Mabel"}]}
                     {:HasAttr [?e :salary ?salary]}
                     [?e]]}
                   {:Filter [?e :age {:Number 14}]}
                   [?e]]}
                 {:Join
                  [{:Join
                    [{:Filter [?e :name {:String "Mabel"}]}
                     {:HasAttr [?e :salary ?salary]}
                     [?e]]}
                   {:Filter [?e :age {:Number 12}]}
                   [?e]]}]]}
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
                  [{:Join
                    [{:Filter [?e :name {:String "Mabel"}]}
                     {:HasAttr [?e :salary ?salary]}
                     [?e]]}
                   {:Filter [?e :age {:Number 14}]}
                   [?e]]}
                 {:Join
                  [{:Join
                    [{:Join
                      [{:Filter [?e :name {:String "Mabel"}]}
                       {:HasAttr [?e :salary ?salary]}
                       [?e]]}
                     {:Filter [?e :age {:Number 12}]}
                     [?e]]}
                   {:Filter [?e :salary {:Number 1000}]}
                   [?e]]}]]}
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
                       [{:HasAttr [?x :edge ?y]}
                        {:Join [{:HasAttr [?x :edge ?z]} {:HasAttr [?z :edge ?y]} [?z]]}]]}
             (compile-query query))))))

(deftest test-negation
  (testing "simple negation"
    (let [query '[:find ?e
                  :where [?e :age 12] (not [?e :name "Mabel"])]]
      (is (= '{:Antijoin
               [{:Filter [?e :age {:Number 12}]} {:Filter [?e :name {:String "Mabel"}]} [?e]]}
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
                [{:HasAttr [0 100 1]}
                 {:Union [[0]
                          {:Filter [0 100 {:String "Mabel"}]}
                          {:Filter [0 ]}]}
                 0]}
               (compile-query query)))))

  (testing "contradiction"
    (let [query '[:find ?e
                  :where (and [?e :name "Mabel"]
                              (not [?e :name "Mabel"]))]]
      (is (= '{:Antijoin
               [{:Filter [?e :name {:String "Mabel"}]}
                {:Filter [?e :name {:String "Mabel"}]}
                [?e]]}
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
                     [{:HasAttr [?x :edge ?y]}
                      {:Join [{:HasAttr [?x :edge ?z]} {:RuleExpr ["recur" [?z ?y]]} [?z]]}]]}
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
                     [{:HasAttr [?x :node ?y]}
                      {:Join [{:HasAttr [?z :edge ?y]} {:RuleExpr ["recur" [?x ?z]]} [?z]]}]]}
           (compile-query query)))))

(deftest test-rules
  (testing "simple rule"
    (let [rules '[[(admin? ?user) [?user :admin? true]]]]
      (is (= #{{:name "admin?" :plan '{:Filter [?user :admin? {:Bool true}]}}}
             (set (compile-rules rules))))))

  (testing "recursive rule"
    (let [rules '[[(propagate ?x ?y) [?x :node ?y]]
                  [(propagate ?x ?y) [?z :edge ?y] (propagate ?x ?z)]]]
      (is (= #{{:name "propagate"
                :plan '{:Union
                        [[?x ?y]
                         [{:HasAttr [?x :node ?y]}
                          {:Join [{:HasAttr [?z :edge ?y]}
                                  {:RuleExpr ["propagate" [?x ?z]]} [?z]]}]]}}}
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
                         {:Join [{:HasAttr [1 100 2]} {:HasAttr [0 100 2]} [2]]}
                         {:HasAttr [1 700 0]}
                         {:Join
                          [{:HasAttr [1 700 3]} {:RuleExpr ["subtype" [3 0]]} [3]]}]]}}}
             (set (compile-rules rules)))))))

(deftest test-predicates
  (testing "simple, top-level predicate"
    (let [query '[:find ?a1 ?a2
                  :where
                  [?user :age ?a1]
                  [?user :age ?a2]
                  [(< ?a1 ?a2)]]]
      (is (= '{:Project
               [{:PredExpr
                 ["LT"
                  [?a1 ?a2]
                  {:Join [{:HasAttr [?user :age ?a1]} {:HasAttr [?user :age ?a2]} [?user]]}]}
                [?a1 ?a2]]}
             (compile-query query)))))

  (testing "predicate union"
    (let [query '[:find ?user ?age
                  :where
                  [?user :age ?age]
                  (or [(>= ?age 21)]
                      [(< ?age 18)])]]
      (is (= '{:Union
               [[?user ?age]
                {:PredExpr ["GTE" [?age] {:HasAttr [?user :age ?age]}]}
                {:PredExpr ["LT" [?age] {:HasAttr [?user :age ?age]}]}]}
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
                [{:PredExpr
                  ["GT"
                   [?t1 ?t2]
                   {:Join
                    [{:Join
                      [{:Join
                        [{:HasAttr [?parent :child ?child1]}
                         {:HasAttr [?child1 :timestamp ?t1]}
                         [?child1]]}
                       {:HasAttr [?parent :child ?child2]}
                       [?parent]]}
                     {:HasAttr [?child2 :timestamp ?t2]}
                     [?child2]]}]}
                 {:PredExpr
                  ["EQ"
                   [?parent ?child1]
                   {:PredExpr
                    ["LT"
                     [?t1 ?t2]
                     {:Join
                      [{:Join
                        [{:Join
                          [{:HasAttr [?parent :child ?child1]}
                           {:HasAttr [?child1 :timestamp ?t1]}
                           [?child1]]}
                         {:HasAttr [?parent :child ?child2]}
                         [?parent]]}
                       {:HasAttr [?child2 :timestamp ?t2]}
                       [?child2]]}]}]}]]}
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
                 [[?user ?age]
                  {:PredExpr ["GT" [?ctr1 ?ctr2]
                              {:Join [{:HasAttr [?child1 :id/ctr ?ctr1]}
                                      {:HasAttr [?child1 :id/ctr ?ctr1]}]}]}
                  {:PredExpr ["LT" [?age] {:HasAttr [?user :age ?age]}]}]}
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
                [{:PredExpr
                  ["GT"
                   [?ctr1 ?ctr2]
                   {:Join
                    [{:Join
                      [{:HasAttr [?child2 :id/ctr ?ctr2]}
                       {:HasAttr [?child2 :id/node ?n2]}
                       [?child2]]}
                     {:Join
                      [{:HasAttr [?child1 :id/ctr ?ctr1]}
                       {:HasAttr [?child1 :id/node ?n1]}
                       [?child1]]}
                     []]}]}
                 {:PredExpr
                  ["EQ"
                   [?ctr1 ?ctr2]
                   {:PredExpr
                    ["GT"
                     [?n1 ?n2]
                     {:Join
                      [{:Join
                        [{:Join
                          [{:HasAttr [?child1 :id/ctr ?ctr1]}
                           {:HasAttr [?child1 :id/node ?n1]}
                           [?child1]]}
                         {:Filter [?child1 :foo {:String "bar"}]}
                         [?child1]]}
                       {:Join
                        [{:HasAttr [?child2 :id/ctr ?ctr2]}
                         {:HasAttr [?child2 :id/node ?n2]}
                         [?child2]]}
                       []]}]}]}]]}
             (compile-query query))))))

;; (deftest test-inputs
;;   (let [query '[:find ?user ?age
;;                 :in ?max-age
;;                 :where
;;                 [?user :age ?age]
;;                 [(< ?age ?max-age)]]]
;;     (is (= '{:PredExpr
;;              ["LT" [?age ?max-age]
;;               {:Join
;;                [{:HasAttr [?user :age ?age]}
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
                      [{:PredExpr
                        ["LT"
                         [?t1 ?t2]
                         {:Join
                          [{:Join
                            [{:Join
                              [{:HasAttr [?op :assign/key ?key]}
                               {:HasAttr [?op :assign/time ?t1]}
                               [?op]]}
                             {:HasAttr [?op2 :assign/key ?key]}
                             [?key]]}
                           {:HasAttr [?op2 :assign/time ?t2]}
                           [?op2]]}]}
                       [?t1 ?key]]}}
             {:name "lww"
              :plan '{:Project
                      [{:Antijoin
                        [{:Join
                          [{:Join
                            [{:HasAttr [?op :assign/time ?t]}
                             {:HasAttr [?op :assign/key ?key]}
                             [?op]]}
                           {:HasAttr [?op :assign/value ?val]}
                           [?op]]}
                         {:RuleExpr ["older?" [?t ?key]]}
                         [?t ?key]]}
                       [?key ?val]]}}}
           (set (compile-rules rules))))))

(deftest test-aggregations
  (testing "min"
    (let [query '[:find ?user (min ?age)
                  :where [?user :age ?age]]]
      (is (= '{:Aggregate ["MIN" {:HasAttr [?user :age ?age]} [?age]]}
             (compile-query query))))))
