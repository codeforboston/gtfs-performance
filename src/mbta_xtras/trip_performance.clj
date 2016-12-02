(ns mbta-xtras.trip-performance
  (:require [clojure.core.async :as async :refer [<! alt! chan go-loop timeout]]
            [clojure.spec :as s]
            [environ.core :refer [env]]
            [com.stuartsierra.component :as component]

            [mbta-xtras.spec :as xs]
            [mbta-xtras.api-spec :as api]
            [mbta-xtras.db :as db]
            [mbta-xtras.protobuf :refer [trip-updates->!]]
            [mbta-xtras.realtime :as rt]
            [mbta-xtras.utils :refer [datetime-for-stamp datetime-for-str
                                      date-str offset-time ->stamp index-by]]
            [monger
             [collection :as mc]
             [core :as mg]]
            [clojure.java.io :as io]

            [taoensso.timbre :refer [error]])
  (:import [java.time LocalDateTime]))


(s/fdef add-stop-delay
        :args (s/cat :scheduled (s/map-of ::xs/stop-sequence ::xs/arrival-time)
                     :stop ::xs/stop-update)
        :ret ::xs/stop-update)

(s/fdef observed-trip-performance
        :args (s/cat :db ::xs/db
                     :trip-id ::xs/trip-id
                     :trip-start ::xs/trip-start)
        :ret (s/coll-of ::xs/stop-update))

(s/fdef trip-performance
        :args (s/cat :db ::xs/db
                     :trip-id ::xs/trip-id
                     :start-date ::xs/start-date)
        :ret (s/coll-of ::xs/stop-update))

(s/fdef process-trips
        :args (s/cat :db ::xs/db
                     :date-trips (s/coll-of ::xs/date-trip))
        :ret nil?)

(s/fdef all-trips
        :args (s/cat :db ::xs/db)
        :ret ::xs/date-trip)


;; Stores when the trip estimates were last updated:
(defonce last-run-info
  (atom (try
          (read-string (slurp "resources/run_info.txt"))
          (catch java.io.FileNotFoundException _exc {})
          (catch RuntimeException _exc {}))))

(defonce write-last-run-info
  (let [m (Object.)]
    (add-watch last-run-info :writer
               (fn [_k _r _o info]
                 (locking m
                   (spit "resources/run_info.txt" (prn-str info)))))))

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
    (map (fn [{at :arrival-time, dt :departure-time,
               ss :stop-sequence, :as stop-time}]
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

(defn scheduled-arrival
  [db start-date trip-id stop-sequence]
  (when-let [st (mc/find-one-as-map db "stop-times"
                                    {:trip-id trip-id
                                     :stop-sequence stop-sequence})]
    (-> start-date
        (datetime-for-str start-date)
        (offset-time (:arrival-time st))
        (->stamp))))

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

(defn all-stops
  "Merge the stops into a single sequence, ordered by :stop-sequence."
  [scheduled actual]
  (map (fn [i] (or (actual i) (scheduled i)))
       (keys scheduled)))

(defn add-estimates
  "Takes a joined sequence of stops, scheduled and unscheduled, ordered by stop
  sequence. Finds any sub-sequences that have no observed arrival time and
  attempts to  estimate their arrival times. Returns a complete collection of
  stops with estimates added."
  ([all-stops & [last-obs]])
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
                          sch-duration (- (:scheduled-arrival next-obs)
                                          (:scheduled-arrival last-obs))]
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

                    (let [delay (- (:arrival-time next-obs)
                                   (:scheduled-arrival next-obs))
                          ]
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
  "Uses data stored in the database to calculate the performance of trips
  between two stops in the specified date range. See docs for a detailed
  walkthrough of the implementation and assumptions."
  [db from-stop to-stop from-dt to-dt]
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
                 :direction (:direction-id trip)})))
          from-stops)))

(defn trips-since
  "Find the unique trips (trip_id + trip_start_date) that have run since the
  given stamp."
  [db stamp]
  (mc/aggregate
   db "trip-stops"
   [{:$match {:$or [{:stamp {:$gte stamp}} {:arrival-time {:$gte stamp}}]}}
    {:$group {:_id {:trip-id "$trip-id", :trip-start "$trip-start"}}}]))

(defn all-trips
  "Returns a lazy sequence of [trip-start trip-id]."
  [db & [match]]
  (map
   (fn [{m :_id}] [(:trip-start m) (:trip-id m)])
   (mc/aggregate
    db "trip-stops"
    (cond->> [{:$group {:_id {:trip-id "$trip-id",
                              :trip-start "$trip-start"}}}]
      match (cons {:$match match}))
    :allow-disk-use true
    :cursor {:batch-size 100})))

(defn process-trips [db date-trips]
  (doseq [[trip-start trip-id] date-trips]
    (doseq [stop (trip-performance db trip-id trip-start)]
      (mc/upsert db "trip-stops" {:id (:id stop)} {:$set stop}))))

(defn process-all-trips! [db]
  (process-trips db (all-trips db)))

(defn post-loop
  ""
  ([db last-run]
   (let [stop (chan)]
     (go-loop [last-run last-run]
       (let [recent-trips (map :_id (trips-since db last-run))]
         (doseq [{:keys [trip-id trip-start]} recent-trips
                 :let [stops (trip-performance db trip-id trip-start)]]
           ))
       (let [stamp (/ (System/currentTimeMillis) 1000)]
         (alt!
           (timeout 3600000) (recur stamp)

           stop nil)))
     stop))
  ([db]
   (post-loop db (/ (System/currentTimeMillis) 1000))))

(defn read-updates-into-db
  "Pull trip stop updates continually and insert them into the given Mongo
  database and column. Returns a stop channel that will halt fetching of results
  and insertion when closed or given a value."
  [db coll]
  (prep-collection db coll)
  (let [in (chan (async/sliding-buffer 100) nil
                 #(error "Encountered an exception in trip update loop:" %))
        stop (trip-updates->! in)
        ;; updates-in (chan (async/sliding-buffer 100))
        ;; stop (trip-updates->! updates-in)
        ;; updates (async/mult updates-in)
        ;; in (async/tap updates (chan (async/sliding-buffer 100)))
        ]
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
