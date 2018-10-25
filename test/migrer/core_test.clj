(ns migrer.core-test
  (:require [migrer.core :as m]
            [clojure.test :refer :all]
            [clojure.java.jdbc :as jdbc]
            [clojure.java.classpath :as cp]))

(comment
  (cp/classpath))

(def test-h2-db
  {:classname "org.h2.Driver"
   :subprotocol "h2:mem"
   :subname "test;DB_CLOSE_DELAY=-1" ;; TODO: Something is fishy here...
   :user "test"
   :password ""})

(use-fixtures :each
  (fn [test]
    (m/init! test-h2-db)
    (test)))

(deftest test-init!
  (is (jdbc/query test-h2-db ["SELECT * FROM migrations"])))

(deftest migrate
  (is (seq (m/migrate test-h2-db))))
