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
  (def rules
    '[[(implicit-dependency ?e ?e')
       [?e :migration.meta/version ?v]
       [?e' :migration.meta/version ?v']
       [(< ?v' ?v)]]
      [(direct-dependency ?e ?e')
       [?e :migration.meta/dependencies ?e']]
      [(runnable? ?e)
       [?e :migration.meta/run? true]]
      [(runnable? ?e)
       [?e :migration.meta/type :migration.type/repeatable]
       [?e :migration.meta/run? false]
       (or
         (implicit-dependency ?e ?e')
         (direct-dependency ?e ?e'))
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
         (or
           (implicit-dependency ?e ?e')
           (direct-dependency ?e ?e'))
         (runnable? ?e')
         (outside-wave? ?e'))]
      [(root-migration ?e)
       (runnable? ?e)
       (not-join [?e]
         (or
           (implicit-dependency ?e ?e')
           (direct-dependency ?e ?e'))
         (runnable? ?e'))]])

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
                    rules))
            "rule: runnable [repeatable chain, explicit]"))

  (let [conn (initialise)]
    (d/transact! conn [{:migration.meta/id "A"
                        :migration.meta/version 1
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true}
                       {:migration.meta/id "B"
                        :migration.meta/version 2
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? false}
                       {:migration.meta/id "C"
                        :migration.meta/version 3
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? false}])
    (tst/is (= #{["A"] ["B"] ["C"]}
               (d/q '[:find ?id
                      :in $ %
                      :where
                      (runnable? ?e)
                      [?e :migration.meta/id ?id]]
                    @conn
                    rules))
            "rule: runnable [repeatable chain, implicit]"))

  (let [conn (initialise)]
    (d/transact! conn [{:migration.meta/id "A"
                        :migration.meta/version 1
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true}
                       {:migration.meta/id "B"
                        :migration.meta/version 2
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? false}
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
                    rules))
            "rule: runnable [repeatable chain, implicit/explicit]"))

  (let [conn (initialise)]
    (d/transact! conn [{:migration.meta/id "A"
                        :migration.meta/version 1
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? false
                        :migration.meta/dependencies []}
                       {:migration.meta/id "B"
                        :migration.meta/version 2
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? false
                        :migration.meta/dependencies [[:migration.meta/id "A"]]}
                       {:migration.meta/id "C"
                        :migration.meta/version 3
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? false
                        :migration.meta/dependencies [[:migration.meta/id "B"]]}])
    (tst/is (= #{["A"] ["B"]}
               (d/q '[:find ?id
                      :in $ %
                      :where
                      [?e :migration.meta/id "C"]
                      (implicit-dependency ?e ?e')
                      [?e' :migration.meta/id ?id]]
                    @conn
                    rules))
            "rule: implicit-dependency #1")

    (tst/is (= #{["A"]}
               (d/q '[:find ?id
                      :in $ %
                      :where
                      [?e :migration.meta/id "B"]
                      (implicit-dependency ?e ?e')
                      [?e' :migration.meta/id ?id]]
                    @conn
                    rules))
            "rule: implicit-dependency #2"))

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
                    rules))
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
                    rules))
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
                    rules))
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
                    rules))
            "rule: next-wave [explicit]"))

  (let [conn (initialise)]
    (d/transact! conn [{:migration.meta/id "A"
                        :migration.meta/version 1
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true
                        :migration.meta/wave 1}
                       {:migration.meta/id "B"
                        :migration.meta/version 2
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/wave 2
                        :migration.meta/run? true}
                       {:migration.meta/id "C"
                        :migration.meta/version 3
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/wave 3
                        :migration.meta/run? true}
                       {:migration.meta/id "D"
                        :migration.meta/version 4
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/wave 4
                        :migration.meta/run? true}
                       {:migration.meta/id "E"
                        :migration.meta/version 5
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true}
                       {:migration.meta/id "F"
                        :migration.meta/version 6
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true}
                       {:migration.meta/id "G"
                        :migration.meta/version 7
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true}
                       {:migration.meta/id "H"
                        :migration.meta/version 8
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true}
                       {:migration.meta/id "I"
                        :migration.meta/version 9
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true}
                       {:migration.meta/id "J"
                        :migration.meta/version 10
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true}
                       {:migration.meta/id "K"
                        :migration.meta/version 11
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true}
                       {:migration.meta/id "L"
                        :migration.meta/version 12
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true}])
    (tst/is (= #{["E"]}
               (d/q '[:find ?id
                      :in $ %
                      :where
                      (next-wave ?e)
                      [?e :migration.meta/id ?id]]
                    @conn
                    rules))
            "rule: next-wave [implicit]"))

  (let [conn (initialise)]
    (d/transact! conn [{:migration.meta/id "A"
                        :migration.meta/version 1
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true
                        :migration.meta/wave 1}
                       {:migration.meta/id "B"
                        :migration.meta/version 2
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/wave 2}
                       {:migration.meta/id "C"
                        :migration.meta/version 3
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/wave 3}
                       {:migration.meta/id "D"
                        :migration.meta/version 4
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/wave 4}
                       {:migration.meta/id "E"
                        :migration.meta/version 5
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true
                        :migration.meta/wave 5}
                       {:migration.meta/id "F"
                        :migration.meta/version 6
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/wave 6}
                       {:migration.meta/id "G"
                        :migration.meta/version 7
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true
                        :migration.meta/wave 7}
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
    (tst/is (= #{["H"] ["I"] ["J"] ["K"]}
               (d/q '[:find ?id
                      :in $ %
                      :where
                      (next-wave ?e)
                      [?e :migration.meta/id ?id]]
                    @conn
                    rules))
            "rule: next-wave [implicit/explicit]"))

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
                    rules))
            "rule: root-migration [explicit]"))

  (let [conn (initialise)]
    (d/transact! conn [{:migration.meta/id "A"
                        :migration.meta/version 1
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/dependencies []}
                       {:migration.meta/id "B"
                        :migration.meta/version 2
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true}
                       {:migration.meta/id "C"
                        :migration.meta/version 3
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true}])
    (tst/is (= #{["A"]}
               (d/q '[:find ?id
                      :in $ %
                      :where
                      (root-migration ?e)
                      [?e :migration.meta/id ?id]]
                    @conn
                    rules))
            "rule: root-migration [implicit]"))

  (let [conn (initialise)]
    (d/transact! conn [{:migration.meta/id "A"
                        :migration.meta/version 1
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
                    rules))
            "rule: root-migration [implicit/explicit]")))

(defn initialise
  ([] (initialise schema))
  ([s]
   (let [conn (d/create-conn s)]
     (d/transact! conn [{:db/ident :migration.type/versioned}
                        {:db/ident :migration.type/repeatable}
                        {:db/ident :migration.type/invalid}])
     conn)))
