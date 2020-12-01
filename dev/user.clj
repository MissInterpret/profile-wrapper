(ns user
  (:require [clojure.pprint :refer [pprint]]
            [profile-wrapper.core :as p]))


(defn thread-wait [n]
  (Thread/sleep (* n 1000))
  (keyword n))


(defn run-profile [count]
  (doseq [i (range count)]
    (println "running " i)
    (let [result  (p/profile p/store-atm
                             ::test
                             (thread-wait 5))]
      (when-not (= (keyword 5) result)
        (println "invalid result!! " result)
        )
      )))
