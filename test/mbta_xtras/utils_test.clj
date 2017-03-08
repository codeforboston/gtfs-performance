(ns mbta-xtras.utils-test
  (:require [mbta-xtras.utils :as $]
            [mbta-xtras.spec :as spec]
            [clojure.test :refer [deftest is]]

            [clojure.test.check :as tc]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.generators :as gen]

            [clojure.spec :as s]
            [clojure.spec.test :as stest]))


(defspec test-date-str
  1000
  (prop/for-all
   [date-str (s/gen ::spec/date-str)]
   (let [dt ($/date-for-str date-str)]
     (= ($/date-str dt) date-str))))


(defspec test-offset-time
  ;; Verify that the time matches.
  1000
  (prop/for-all
   [date-str (s/gen ::spec/date-str)
    time-str (s/gen ::spec/time-str)]
   (let [date ($/datetime-for-str date-str)
         dt ($/offset-time date time-str)]
     (= (.format $/clock-time-format dt) time-str))))


(defspec test-distinct-with
  1000
  (prop/for-all
   [vals (gen/vector (gen/choose 1 50) 100)]
   (every?
    #(= (count (second %)) 1)
    (group-by
     :k
     (into []  ($/distinct-with :k) (map (fn [v] {:k v}) vals))))))

