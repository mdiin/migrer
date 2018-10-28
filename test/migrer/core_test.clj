(ns migrer.core-test
  (:require [migrer.core :as m]
            [clojure.test :refer :all]
            [clojure.java.jdbc :as jdbc]))

(def ^:dynamic test-h2-db nil)

(defn make-test-h2-db
  []
  (let [db-name (gensym "test")]
    {:classname "org.h2.Driver"
     :subprotocol "h2:mem"
     :subname (str db-name ";DB_CLOSE_DELAY=100")
     :user "test"
     :password ""}))

(use-fixtures :once
  (fn [tests]
    (binding [test-h2-db (make-test-h2-db)]
      (m/init! test-h2-db)
      (tests))))

(deftest test-init!
  (is (jdbc/query test-h2-db ["SELECT * FROM migrations"])))

(deftest migrate!
  (is (seq (m/migrate! test-h2-db)))
  (is (every? #{{:id 1 :name "Tomato" :city_id 1}
                {:id 2 :name "Cucumber" :city_id 2}
                {:id 3 :name "Garlic" :city_id nil}}
              (jdbc/query test-h2-db ["SELECT * FROM products"])))
  (is (every? #{{:id 1 :name "Sin City"}
                {:id 2 :name "Gotham City"}}
              (jdbc/query test-h2-db ["SELECT * FROM cities"])))
  (is (every? #{{:id 1 :product_name "Tomato" :city_name "Sin City"}
                {:id 2 :product_name "Cucumber" :city_name "Gotham City"}}
              (jdbc/query test-h2-db ["SELECT * FROM products_in_cities"]))))
