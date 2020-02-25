(ns migrer.test.performance-test
  (:require [migrer.core :as m]
            [migrer.facts :as f]
            [clojure.test :refer :all]
            [datascript.core :as d]))

(deftest performance-test
  (testing "migration-eids-in-application-order"
    (let [conn (f/initialise)]
      (d/transact!
        conn
        (into []
              (comp (map (fn [id]
                           (let [temp {:migration.meta/id id
                                       :migration.meta/type :migration.type/repeatable
                                       :migration.meta/run? true}]
                             (if-not (= id 1)
                               (merge temp
                                      {:migration.meta/dependencies [[:migration.meta/id (dec id)]]})
                               temp))))
                    (take 1000))
              (iterate inc 1)))
      (let [expected (d/q '[:find ?e
                            :in $
                            :where [?e :migration.meta/id _]]
                          @conn)
            actual (set (m/migration-eids-in-application-order conn))]
        (is (count actual))))))
