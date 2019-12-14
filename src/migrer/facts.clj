(ns migrer.facts
  (:require
   [datascript.core :as d]))

(def schema
  {:migration.meta/id {:db/unique :db.unique/identity}
   :migration.meta/description {:db/cardinality :db.cardinality/one}
   :migration.meta/version {:db/cardinality :db.cardinality/one}
   :migration.meta/dependencies {:db/cardinality :db.cardinality/many
                                 :db/valueType :db.type/ref}
   :migration.meta/pre {:db/cardinality :db.cardinality/many}
   :migration.meta/post {:db/cardinality :db.cardinality/many}
   :migration.meta/run? {:db/cardinality :db.cardinality/one}
   :migration.meta/type {:db/cardinality :db.cardinality/one
                         :db/valueType :db.type/ref}
   :migration.raw/filename {:db/cardinality :db.cardinality/one}
   :migration.raw/dependencies {:db/cardinality :db.cardinality/many}
   :migration.raw/sql {:db/cardinality :db.cardinality/one}})

(defn- lt?
  [a b]
  (= -1 (compare a b)))

(def rules
  '[[(earlier-version ?e1 ?e2)
     [?e1 :migration.meta/version ?e1-v]
     [?e2 :migration.meta/version ?e2-v]
     [(migrer.core/lt? ?e1-v ?e2-v)]]
    [(dependency-of ?e1 ?e2)
     [?e1 :migration.meta/dependencies ?e2]]
    [(dependency-of ?e1 ?e3)
     [?e2 :migration.meta/dependencies ?e3]
     (dependency-of ?e1 ?e2)]
    [(prior-to ?e1 ?e2)
     (earlier-version ?e1 ?e2)]
    [(prior-to ?e1 ?e2)
     (dependency-of ?e2 ?e1)]
    [(root-level? ?e ?ep)
     [?e :migration.meta/id]
     (not
      (prior-to ?ep ?e)
      (should-run? ?ep))]
    [(should-run? ?e)
     (or
      [?e :migration.meta/type :migration.type/versioned]
      [?e :migration.meta/type :migration.type/repeatable])
     [?e :migration.meta/run? true]]
    [(should-run? ?e)
     [?e :migration.meta/type :migration.type/seed]
     [?e :migration.meta/run? true]
     [?e :migration.meta/dependencies ?edep]
     (not
      [(nil? ?edep)])]
    [(should-run? ?e)
     [?e :migration.meta/type :migration.type/repeatable]
     [?e :migration.meta/run? false]
     (prior-to ?e-prior ?e)
     (should-run? ?e-prior)]])

(defn initialise
  ([] (initialise schema))
  ([s]
   (let [conn (d/create-conn s)]
     (d/transact! conn [{:db/ident :migration.type/versioned}
                        {:db/ident :migration.type/repeatable}
                        {:db/ident :migration.type/seed}
                        {:db/ident :migration.type/invalid}])
     conn)))
