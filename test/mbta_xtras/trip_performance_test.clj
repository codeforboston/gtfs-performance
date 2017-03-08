(ns mbta-xtras.trip-performance-test
  (:require [mbta-xtras.trip-performance :as tp]
            [mbta-xtras.spec :as spec]

            [clojure.spec.test :as stest]

            [clojure.test.check :as tc]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.spec :as s]))


(defn is-sorted? [xs]
  (every? true? (map <= xs (rest xs))))

(defn key-comparator [k]
  #(compare (k %) (k %2)))

(def all-stops-gen
  "Generator that produces a semi-plausible sequence of merged stop observations
  and scheduled stops. Stops have the minimap keyset needed for processing by
  add-estimates. Ordered by stop-sequence."
  (gen/fmap (fn [[scheduled-time offset stops]]
              (let [start-time (+ scheduled-time offset)]
                (into (sorted-set-by (key-comparator :stop-sequence))
                      (map (fn [[scheduled? eh? stop-seq rnd]]
                             (let [sch (+ scheduled-time (* stop-seq 30))]
                               (merge
                                {:stop-sequence stop-seq
                                 :scheduled? scheduled?
                                 :scheduled-arrival sch
                                 :scheduled-departure sch}
                                (when-not scheduled?
                                  (let [arr (+ start-time (* stop-seq 30) rnd)]
                                    {:arrival-time arr
                                     :departure-time arr
                                     :delay (- arr sch)}))))))
                      stops)))
            ;; Inputs:
            (gen/tuple (gen/choose 1488000000 1800000000)
                       gen/int
                       (gen/list (gen/tuple gen/boolean
                                            gen/boolean
                                            gen/pos-int
                                            (gen/choose 0 20))))))

(def all-stops-some-observations-gen
  (gen/such-that #(not (every? :scheduled? %)) all-stops-gen))

(def all? (partial every? true?))

(defn all-pairs? [pred xs]
  (all? (map pred xs (rest xs))))

(defspec test-stops-generator
  ;; Make sure the generator produces good data
  100
  (prop/for-all [stops all-stops-gen]
                (let [times (map :scheduled-arrival stops)]
                  (all-pairs? <= times))))

#_
(defspec test-)

(def *stops3
  [{:stop-sequence 0, :scheduled? false, :scheduled-arrival 1488000000, :scheduled-departure 1488000000, :arrival-time 1488000031, :departure-time 1488000031, :delay 31} {:stop-sequence 22, :scheduled? true, :scheduled-arrival 1488000660, :scheduled-departure 1488000660} {:stop-sequence 23, :scheduled? false, :scheduled-arrival 1488000690, :scheduled-departure 1488000690, :arrival-time 1488000705, :departure-time 1488000705, :delay 15}])

(def ! complement)


(defspec test-add-estimates
  ;; When estimates are added, do the arrival times increase monotonically?
  100
  (prop/for-all [stops all-stops-some-observations-gen]
                (let [estimates (tp/add-estimates stops)]
                  ;; Verify that all the stops has arrival times
                  (every? (! nil?) (map :arrival-time estimates))
                  ;; Verify that the arrival times are in order
                  (all-pairs? <= (map :arrival-time estimates))

                  ;;
                  )))

#_
(tc/quick-check 100 (prop/for-all [stops all-stops-gen]
                                  (let [estimates (tp/add-estimates stops)]
                                    (or (every? nil? (map :arrival-time estimates))
                                        ;; Verify that the arrival times are in order
                                        (all-pairs? <= (map :arrival-time estimates))))))

