(ns migrer.render
  (:require
   [clojure.test :as tst :refer [with-test]]
   [dorothy.core :as dot]
   [dorothy.jvm :as jvm]))

(with-test
  (defn dotviz
    [depinfo]
    (reduce
     (fn [dot [i deps]]
       (let [node-id (keyword (str i))
             dep-node-ids (map (comp keyword str) deps)]
         (into (conj dot [node-id])
               (map #(vector % node-id))
               dep-node-ids)))
     []
     depinfo))

  (let [deps #{[1 #{}] [2 #{1}] [3 #{1 2}] [4 #{2 3}] [5 #{2 3 4}] [6 #{5}] [7 #{3 6}] [8 #{4 5 7}] [9 #{1 2 3 4 5 6 7 8}]}]
    (tst/is (= (set (dotviz deps))
               (set [[:1]
                     [:2]
                     [:1 :2]
                     [:3]
                     [:1 :3]
                     [:2 :3]
                     [:4]
                     [:2 :4]
                     [:3 :4]
                     [:5]
                     [:2 :5]
                     [:3 :5]
                     [:4 :5]
                     [:6]
                     [:5 :6]
                     [:7]
                     [:3 :7]
                     [:6 :7]
                     [:8]
                     [:4 :8]
                     [:5 :8]
                     [:7 :8]
                     [:9]
                     [:1 :9]
                     [:2 :9]
                     [:3 :9]
                     [:4 :9]
                     [:5 :9]
                     [:6 :9]
                     [:7 :9]
                     [:8 :9]])))))

(comment
  (let [deps #{[1 #{}] [2 #{1}] [3 #{1 2}] [4 #{2 3}] [5 #{2 3 4}] [6 #{5}] [7 #{3 6}] [8 #{4 5 7}] [9 #{1 2 3 4 5 6 7 8}]}]
    (jvm/show! (dot/dot (dot/digraph (dotviz deps))))))

(defn show-dependency-graph
  [depinfo]
  (-> depinfo
      (dotviz)
      (dot/digraph)
      (dot/dot)
      (jvm/show!)))
