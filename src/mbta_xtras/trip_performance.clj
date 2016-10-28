(ns mbta-xtras.trip-performance
  (:require [clojure.core.async :as async :refer [<! go-loop]]
            [clojure.spec :as s]
            [environ.core :refer [env]]
            [com.stuartsierra.component :as component]

            [mbta-xtras.api-spec :as api]
            [mbta-xtras.protobuf :refer [trip-updates->!]]
            [mbta-xtras.utils :refer [datetime-for-stamp datetime-for-str
                                      date-str offset-time ->stamp]]
            [monger
             [collection :as mc]
             [core :as mg]])
  (:import [java.time LocalDateTime]))


;; Function definitions:
(defn prep-collection [db mongo-coll]
  (mc/ensure-index db mongo-coll
                   (array-map :stop-id 1, :arrival-time 1))
  (mc/ensure-index db mongo-coll (array-map :id 1) {:unique true}))

(defn trip-ids-for-date
  ([db date-str]
   (into #{} (map :trip-id)
         (mc/find-maps db "trip-stops" {:trip-start date-str})))
  ([db]
   (trip-ids-for-date db (date-str (LocalDateTime/now)))))

(defn- transduce-stop-times
  [start-date]
  (let [ref-date (datetime-for-str start-date)]
    (map (fn [{at :arrival-time, dt :departure-time, ss :stop-sequence, :as stop-time}]
           [(Integer/parseInt ss)
            (-> (dissoc stop-time :_id)
                (assoc :stop-sequence (Integer/parseInt ss)
                       :scheduled-arrival (->stamp (offset-time ref-date at))
                       :scheduled-departure (->stamp (offset-time ref-date dt))
                       :scheduled? true))]))))

(defn stop-times
  [db trip-id start-date]
  (into (sorted-map)
        (transduce-stop-times start-date)
        (mc/find-maps db "stop-times" {:trip-id trip-id})))

(defn scheduled-arrivals
  "Returns a map of scheduled stop arrival times, indexed by the stop sequence."
  [db trip-id start-date]
  (into (sorted-map)
        (let [ref-date (datetime-for-str start-date)]
          (map (fn [{at :arrival-time, ss :stop-sequence}]
                 [(Integer/parseInt ss) (->stamp (offset-time ref-date at))])))
        (mc/find-maps db "stop-times" {:trip-id trip-id})))

(defn add-stop-delay
  [scheduled stop]
  (when-let [sched (scheduled (:stop-sequence stop))]
    (merge
     (dissoc stop :_id)
     {:delay (- (:arrival-time stop) sched)
      :scheduled-arrival sched})))

(defn observed-trip-performance
  [db trip-id start-date]
  (let [scheduled (scheduled-arrivals db trip-id start-date)]
    (->> (mc/find-maps db "trip-stops" {:trip-id trip-id,
                                        :trip-start start-date})
         (keep (partial add-stop-delay scheduled))
         (sort-by :stop-sequence))))

(defn index-by [k coll]
  (into {} (map (juxt k identity)) coll))


(defn all-stops
  "Merge the stops into a single sequence, ordered by :stop-sequence."
  [scheduled actual]
  (map (fn [i] (or (actual i) (scheduled i)))
       (range 1 (inc (count scheduled)))))

(defn add-estimates
  "Takes a joined sequence of stops, scheduled and unscheduled, ordered by stop
  sequence. Finds any sub-sequences that have no observed arrival time and
  attempts to  estimate their arrival times."
  [all-stops & [last-obs]]
  (when-let [l (seq all-stops)]
    (let [[sched actual] (split-with :scheduled? l)]
      (if (seq sched)
        (concat (if-let [next-obs (first actual)]
                  (if-let [last-arrival (:arrival-time last-obs)]
                    ;; We have observed arrival times for the stops flanking this
                    ;; subsequence of scheduled stops, so we'll 'spread' the
                    ;; (positive or negative) delay over the stops between the
                    ;; observed stops. Use the scheduled to determine the
                    ;; proportion of the total trip that each stop represents and
                    ;; scale the additional delay appropriately.
                    (let [obs-duration (- (:arrival-time next-obs) last-arrival)
                          sch-duration (- (:scheduled-arrival next-obs) (:scheduled-arrival last-obs))]
                      (map (fn [scheduled-stop]
                             (let [delay (* (/ (- (:scheduled-arrival scheduled-stop)
                                                  (:scheduled-arrival last-obs))
                                               sch-duration)
                                            obs-duration)]
                               (assoc scheduled-stop
                                      :arrival-time (+ last-arrival delay)
                                      :delay delay
                                      :estimation-method "interpolated"
                                      :estimated? true)))
                           sched))
                    ;; We don't have information about the last arrival, but we do
                    ;; have the next observed arrival, so shift the whole schedule over
                    (let [shift (- (:arrival-time next-obs)
                                   (:scheduled-arrival next-obs))]
                      (map (fn [scheduled-stop]
                             (assoc scheduled-stop
                                    :arrival-time (+ shift
                                                     (:scheduled-arrival scheduled-stop))
                                    :delay shift
                                    :estimation-method "shifted"
                                    :estimated? true))
                           sched)))

                  ;; We have only the schedule! Don't bother estimating.
                  sched)

                (add-estimates actual))

        (let [[actual rest] (split-with (complement :scheduled?) actual)]
          (concat actual (add-estimates rest (last actual))))))))

(defn trip-performance
  "Take the observed trip stop updates and the scheduled stop arrival times and
  use them to calculate how far ahead or behind schedule each stop of the trip
  was. When there is no information about a trip's arrival time at a stop,
  estimate it using either interpolation or shifting, depending on the
  information available."
  [db trip-id start-date]
  (let [scheduled (stop-times db trip-id start-date)
        trip-stops (->> (mc/find-maps db "trip-stops" {:trip-id trip-id
                                                       :trip-start start-date})
                        (keep (partial add-stop-delay (comp :scheduled-arrival scheduled)))
                        (index-by :stop-sequence))]
    (add-estimates (all-stops scheduled trip-stops))))


(defn travel-times
  [db from-stop to-stop from-dt to-dt]
  ;; See docs for a more detailed walkthrough
  (let [from-stops (mc/find-maps db "trip-stops" {:stop-id from-stop
                                                  :arrival-time {:$gte from-dt
                                                                 :$lte to-dt}})
        trip-ids (into #{} (map :trip-id) from-stops)
        to-stops (mc/find-maps db "trip-stops" {:stop-id to-stop
                                                :arrival-time {:$gte from-dt
                                                               :$lte to-dt}
                                                :trip-id {:$in trip-ids}})
        trip-ends (index-by (juxt :trip-id :trip-start) to-stops)
        trips (index-by :trip-id (mc/find-maps db "trips"
                                               {:trip-id {:$in trip-ids}}
                                               {:direction-id 1, :route-id 1,
                                                :trip-id 1}))]

    ;; For now, let's assume that all trip stops have gone through
    ;; post-processing and have :scheduled-arrival, etc.
    (keep (fn [from-stop]
            (when-let [to-stop (trip-ends [(:trip-id from-stop)
                                           (:start-date from-stop)])]
              (let [bench (- (:scheduled-arrival to-stop)
                             (:scheduled-departure from-stop))
                    trip (trips (:trip-id from-stop))]
                {:dep-dt (str (:departure-time from-stop))
                 :arr-dt (str (:arrival-time to-stop))
                 :travel-time-sec (- (:arrival-time to-stop) (:departure-time
                                                              from-stop))
                 :estimated? (or (:estimated? from-stop) (:estimated? to-stop))
                 :benchmark-travel-time-sec bench
                 :route-id (:route-id trip)
                 :direction (:direction-id trip)}))))))

(defn read-updates-into-db
  "Pull trip stop updates continually and insert them into the given Mongo
  database and column. Returns a stop channel that will halt fetching of results
  and insertion when closed or given a value."
  [db coll]
  (prep-collection db coll)
  (let [in (async/chan (async/sliding-buffer 100))
        stop (trip-updates->! in)]
    (go-loop []
      (when-let [trip-stop (<! in)]
        (mc/upsert db coll
                   {:id (:id trip-stop)}
                   {:$set trip-stop})
        (recur)))
    stop))


(defrecord TripPerformanceRecorder [coll]
  component/Lifecycle
  (start [this]
    (assoc this
           :stop-chan (read-updates-into-db (-> this :mongo :db) coll)))

  (stop [this]
    (some-> (:stop-chan this) (async/close!))
    (dissoc this :stop-chan)))


(defn make-recorder []
  (->TripPerformanceRecorder (env :trip-stops-collection "trip-stops")))

