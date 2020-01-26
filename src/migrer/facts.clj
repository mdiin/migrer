(ns migrer.facts
  (:require
    [clojure.test :as tst :refer [with-test]]
    [datascript.core :as d]))

(def schema
  {:migration.meta/id {:db/unique :db.unique/identity}
   :migration.meta/description {:db/cardinality :db.cardinality/one}
   :migration.meta/version {:db/cardinality :db.cardinality/one}
   :migration.meta/dependencies {:db/cardinality :db.cardinality/many
                                 :db/valueType :db.type/ref}
   :migration.meta/run? {:db/cardinality :db.cardinality/one}
   :migration.meta/type {:db/cardinality :db.cardinality/one
                         :db/valueType :db.type/ref}
   :migration.meta/wave {:db/cardinality :db.cardinality/one}
   :migration.raw/filename {:db/cardinality :db.cardinality/one}
   :migration.raw/dependencies {:db/cardinality :db.cardinality/many}
   :migration.raw/sql {:db/cardinality :db.cardinality/one}})

(defn- lt?
  [a b]
  (= -1 (compare a b)))

(declare initialise)

(with-test
  (def new-rules
    '[[(direct-dependency ?e ?e')
       [?e :migration.meta/dependencies ?e']]
      [(runnable? ?e)
       [?e :migration.meta/run? true]]
      [(runnable? ?e)
       [?e :migration.meta/type :migration.type/repeatable]
       [?e :migration.meta/run? false]
       (direct-dependency ?e ?e')
       (runnable? ?e')]
      [(in-wave? ?e)
       [?e :migration.meta/wave _]]
      [(outside-wave? ?e)
       [?e :migration.meta/id _]
       (not (in-wave? ?e))]
      [(next-wave ?e)
       (runnable? ?e)
       (outside-wave? ?e)
       (not-join [?e]
         (direct-dependency ?e ?e')
         (runnable? ?e')
         (outside-wave? ?e'))]
      [(root-migration ?e)
       (runnable? ?e)
       (not-join [?e]
         (direct-dependency ?e ?e')
         (runnable? ?e'))]
      [(root-migration ?e)
       (runnable? ?e)
       [(missing? $ ?e :migration.meta/dependencies)]]])

  (let [conn (initialise)]
    (d/transact! conn [{:migration.meta/id "A"
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true
                        :migration.meta/dependencies []}
                       {:migration.meta/id "B"
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? false
                        :migration.meta/dependencies [[:migration.meta/id "A"]]}
                       {:migration.meta/id "C"
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? false
                        :migration.meta/dependencies [[:migration.meta/id "B"]]}])
    (tst/is (= #{["A"] ["B"] ["C"]}
               (d/q '[:find ?id
                      :in $ %
                      :where
                      (runnable? ?e)
                      [?e :migration.meta/id ?id]]
                    @conn
                    new-rules))
            "rule: runnable [repeatable chain]"))

  (let [conn (initialise)]
    (d/transact! conn [{:migration.meta/id "A"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? false
                        :migration.meta/dependencies []}
                       {:migration.meta/id "B"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? false
                        :migration.meta/dependencies [[:migration.meta/id "A"]]}
                       {:migration.meta/id "C"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? false
                        :migration.meta/dependencies [[:migration.meta/id "B"]]}])
    (tst/is (= #{["B"]}
               (d/q '[:find ?id
                      :in $ %
                      :where
                      [?e :migration.meta/id "C"]
                      (direct-dependency ?e ?e')
                      [?e' :migration.meta/id ?id]]
                    @conn
                    new-rules))
            "rule: direct-dependency"))

  (let [conn (initialise)]
    (d/transact! conn [{:migration.meta/id "A"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? false
                        :migration.meta/wave 1
                        :migration.meta/dependencies []}
                       {:migration.meta/id "B"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/wave 2
                        :migration.meta/run? false
                        :migration.meta/dependencies [[:migration.meta/id "A"]]}
                       {:migration.meta/id "C"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? false
                        :migration.meta/dependencies [[:migration.meta/id "B"]]}])
    (tst/is (= #{["C"]}
               (d/q '[:find ?id
                      :in $ %
                      :where
                      (outside-wave? ?e)
                      [?e :migration.meta/id ?id]]
                    @conn
                    new-rules))
            "rule: outside-wave?"))

  (let [conn (initialise)]
    (d/transact! conn [{:migration.meta/id "A"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? false
                        :migration.meta/wave 1
                        :migration.meta/dependencies []}
                       {:migration.meta/id "B"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/wave 2
                        :migration.meta/run? false
                        :migration.meta/dependencies [[:migration.meta/id "A"]]}
                       {:migration.meta/id "C"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? false
                        :migration.meta/dependencies [[:migration.meta/id "B"]]}])
    (tst/is (= #{["A"] ["B"]}
               (d/q '[:find ?id
                      :in $ %
                      :where
                      (in-wave? ?e)
                      [?e :migration.meta/id ?id]]
                    @conn
                    new-rules))
            "rule: in-wave?"))

  (let [conn (initialise)]
    (d/transact! conn [{:migration.meta/id "A"
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true
                        :migration.meta/wave 1
                        :migration.meta/dependencies []}
                       {:migration.meta/id "B"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/wave 1
                        :migration.meta/dependencies []}
                       {:migration.meta/id "C"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "A"]
                                                      [:migration.meta/id "B"]]}
                       {:migration.meta/id "D"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "B"]]}
                       {:migration.meta/id "E"
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "A"]]}
                       {:migration.meta/id "F"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/wave 1
                        :migration.meta/dependencies []}
                       {:migration.meta/id "G"
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "C"]
                                                      [:migration.meta/id "F"]
                                                      [:migration.meta/id "D"]]}
                       {:migration.meta/id "H"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "G"]
                                                      [:migration.meta/id "E"]
                                                      [:migration.meta/id "A"]
                                                      [:migration.meta/id "B"]]}
                       {:migration.meta/id "I"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "A"]
                                                      [:migration.meta/id "B"]
                                                      [:migration.meta/id "F"]
                                                      [:migration.meta/id "G"]]}
                       {:migration.meta/id "J"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "B"]
                                                      [:migration.meta/id "C"]]}
                       {:migration.meta/id "K"
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "B"]]}
                       {:migration.meta/id "L"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "C"]
                                                      [:migration.meta/id "F"]
                                                      [:migration.meta/id "H"]]}])
    (tst/is (= #{["C"] ["D"] ["E"] ["K"]}
               (d/q '[:find ?id
                      :in $ %
                      :where
                      (next-wave ?e)
                      [?e :migration.meta/id ?id]]
                    @conn
                    new-rules))
            "rule: next-wave"))

  (let [conn (initialise)]
    (d/transact! conn [{:migration.meta/id "A"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/dependencies []}
                       {:migration.meta/id "B"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "A"]]}
                       {:migration.meta/id "C"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/dependencies []}])
    (tst/is (= #{["A"] ["C"]}
               (d/q '[:find ?id
                      :in $ %
                      :where
                      (root-migration ?e)
                      [?e :migration.meta/id ?id]]
                    @conn
                    new-rules))
            "rule: root-migration")))

(defn initialise
  ([] (initialise schema))
  ([s]
   (let [conn (d/create-conn s)]
     (d/transact! conn [{:db/ident :migration.type/versioned}
                        {:db/ident :migration.type/repeatable}
                        {:db/ident :migration.type/seed}
                        {:db/ident :migration.type/invalid}])
     conn)))
