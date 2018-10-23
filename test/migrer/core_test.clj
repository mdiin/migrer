(ns migrer.core-test
  (:require [migrer.core :as m]
            [clojure.test :refer :all]))

(deftest test-init!
  (let [state (atom [])]
    (with-redefs [clojure.java.jdbc/execute! (fn mock-execute!
                                               ([_ sql]
                                                (swap! state conj sql))
                                               ([db sql opts]
                                                (swap! state conj sql)))]
      (m/init! {})
      (is (= @state
             [["CREATE TABLE IF NOT EXISTS migrations (type varchar(32) NOT NULL, version varchar(32) NOT NULL, filename varchar(256) NOT NULL, hash varchar(256), performed_at timestamp with time zone NOT NULL DEFAULT now())"]
              ["CREATE INDEX IF NOT EXISTS migrations_type_idx ON migrations (type);"]])))))
