(ns mbta-xtras.trip-performance
  (:require [clojure.core.async :as async :refer [<! alt! chan go-loop
                                                  sliding-buffer tap timeout]]
            [clojure.core.memoize :refer [lru]]
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

            [taoensso.timbre :refer [error info]]
            [mbta-xtras.utils :as $])
  (:import [java.time LocalDate ZoneId]))


(s/fdef stop-times
        :args (s/cat :db ::xs/db
                     :trip-id ::xs/trip-id
                     :trip-start ::xs/trip-start)
        :ret (s/coll-of ::xs/scheduled-stop))

(s/fdef scheduled-arrivals
        :args (s/cat :db ::xs/db
                     :trip-id ::xs/trip-id
                     :trip-start ::xs/trip-start)
        :ret ::xs/scheduled-arrivals)

(s/fdef add-stop-delay
        :args (s/cat :scheduled (s/map-of ::xs/stop-sequence ::xs/arrival-time)
                     :stop ::xs/stop-update)
        :ret ::xs/stop-update)

(s/fdef observed-trip-performance
        :args (s/cat :db ::xs/db
                     :trip-id ::xs/trip-id
                     :trip-start ::xs/trip-start)
        :ret (s/coll-of ::xs/stop-update))

(s/fdef all-stops
        :args (s/cat :scheduled ::xs/scheduled-arrivals
                     :actual ::xs/stop-updates)
        :ret ::xs/stop-updates)

#_
(s/fdef add-estimates
        :args (s/cat :stops (s/coll-of (s/or ::xs/scheduled-stop
                                             ::xs/stop-update)))
        :ret (s/coll-of (s/and ::api)))

(s/fdef trip-performance
        :args (s/cat :db ::xs/db
                     :trip-id ::xs/trip-id
                     :trip-start ::xs/trip-start)
        :ret (s/coll-of ::xs/stop-update))

(s/fdef process-trips
        :args (s/cat :db ::xs/db
                     :trip-instances (s/coll-of ::xs/trip-instance))
        :ret nil?)

(s/fdef all-trips-for-day
        :args (s/cat :db ::xs/db
                     :trip-start ::xs/trip-start)
        :ret (s/coll-of ::xs/trip-instance))

(s/fdef trips-since
        :args (s/cat :db ::xs/db
                     :stamp ::xs/stamp)
        :ret (s/coll-of ::xs/trip-instance))

(s/fdef recent-trips
        :args (s/cat :db ::xs/db
                     :duration int?)
        :ret (s/coll-of ::xs/trip-instance))



(s/fdef add-stop-delay
        :args (s/cat :scheduled ::xs/scheduled-arrivals
                     :stop ::xs/stop-update)
        :ret (s/and ::xs/stop-update ::xs/scheduled-stop))
;; End specs


;; Function definitions:
(defn stop-times
  ([db trip-id trip-start]
   (let [ref-date (datetime-for-str trip-start)
         xf (map (fn [{at :arrival-time, dt :departure-time,
                       ss :stop-sequence, :as stop-time}]
                   [($/->int ss)
                    (-> (dissoc stop-time :_id)
                        (assoc :stop-sequence ($/->int ss)
                               :scheduled-arrival (->stamp (offset-time ref-date at))
                               :scheduled-departure (->stamp (offset-time ref-date dt))
                               :scheduled? true
                               :trip-id trip-id
                               :trip-start trip-start))]))]
     (into (sorted-map) xf (db/stop-times-for-trip db trip-id))))
  ([db [trip-id trip-start]]
   (stop-times db trip-id trip-start)))

(defn scheduled-arrivals
  "Returns a map of scheduled stop arrival times, indexed by the stop sequence.
  This calculates the arrival times of the trip regardless of whether the trip
  is actually scheduled to run/did run on the given date."
  ([db trip-id trip-start]
   (into (sorted-map)
         (let [ref-date (datetime-for-str trip-start)]
           (map (fn [{at :arrival-time, ss :stop-sequence}]
                  [ss (->stamp (offset-time ref-date at))])))
         (db/stop-times-for-trip db trip-id)))
  ([db [trip-id trip-start]]
   (scheduled-arrivals db trip-id trip-start)))

(defn add-stop-delay
  [scheduled stop]
  (when-let [sched (scheduled (:stop-sequence stop))]
    (merge
     (dissoc stop :_id)
     (when-let [at (:arrival-time stop)]
       {:delay (- at sched)})
     {:scheduled-arrival sched})))

(defn observed-trip-performance
  "Returns a sequence of trip stops for a trip-id on a given start date,
  omitting estimates."
  [db trip-id start-date]
  (let [scheduled (scheduled-arrivals db trip-id start-date)]
    (->> (mc/find-maps db "trip-stops" {:trip-id trip-id,
                                        :trip-start start-date
                                        :estimated? {:$ne true}})
         (keep (partial add-stop-delay scheduled))
         (sort-by :stop-sequence))))

(defn all-stops
  "Merge the stops into a single sequence, ordered by :stop-sequence.
  `scheduled` must be a sorted map."
  [scheduled actual]
  (map (fn [i] (or (actual i)
                   (dissoc (scheduled i) :arrival-time :departure-time)))
       (keys scheduled)))

(defn add-estimates
  "Takes a joined sequence of stops, scheduled and unscheduled, ordered by stop
  sequence. Finds any sub-sequences that have no observed arrival time and
  attempts to estimate their arrival times. Returns a complete collection of
  stops with estimates added when possible. Recuse until all-stops is completely
  consumed."
  [all-stops & [last-obs]]
  (when-let [l (seq all-stops)]
    (let [[sched actual] (split-with :scheduled? l)]
      (if (seq sched)
        (concat (if-let [next-obs (first actual)]
                  (if last-obs
                    ;; We have observed arrival times for the stops flanking this
                    ;; subsequence of scheduled stops, so we'll 'spread' the
                    ;; (positive or negative) delay over the stops between the
                    ;; observed stops. Use the scheduled to determine the
                    ;; proportion of the total trip that each stop represents and
                    ;; scale the additional delay appropriately.
                    (let [last-arrival (:arrival-time last-obs)
                          obs-duration (- (:arrival-time next-obs) last-arrival)
                          sch-duration (- (:scheduled-arrival next-obs)
                                          (:scheduled-arrival last-obs))]
                      (map (fn [scheduled-stop]
                             (let [delay (if (zero? sch-duration)
                                           0
                                           (* (/ (- (:scheduled-arrival scheduled-stop)
                                                    (:scheduled-arrival last-obs))
                                                 sch-duration)
                                              obs-duration))]
                               (assoc scheduled-stop
                                      :id (str (:trip-id scheduled-stop) "-"
                                               (:trip-start scheduled-stop) "-"
                                               (:stop-id scheduled-stop))
                                      :arrival-time (long (+ last-arrival delay))
                                      :departure-time (long (+ (:departure-time
                                                                last-obs) delay))
                                      :delay (int delay)
                                      :estimation-method "interpolated"
                                      :estimated? true)))
                           sched))

                    ;; There's a next observation but no last (previous) observation.

                    ;; For estimation purposes, assume that the trip began on
                    ;; time (delay=0) and became later/earlier at each stop in
                    ;; proportion to the scheduled time between stops.
                    (let [obs-delay (- (:arrival-time next-obs) (:scheduled-arrival next-obs))
                          sch-start (:scheduled-arrival (first sched))
                          sch-duration (- (:scheduled-arrival next-obs) sch-start)]
                      (map (fn [{:keys [scheduled-arrival scheduled-departure]
                                 :as scheduled-stop}]
                             (let [stop-delay (if (zero? sch-duration)
                                                0
                                                (* (/ (- scheduled-arrival sch-start)
                                                      sch-duration)
                                                   obs-delay))]

                               (assoc scheduled-stop
                                      :arrival-time (long (+ scheduled-arrival stop-delay))
                                      :departure-time (long (+ scheduled-departure stop-delay))
                                      :delay (int stop-delay)
                                      :shift-delay (int obs-delay)
                                      :estimated? true)))
                           sched)))

                  ;; We have only the schedule! Don't bother estimating.
                  sched)

                (add-estimates actual last-obs))

        (let [[actual rest] (split-with (complement :scheduled?) actual)
              non-skipped (filter :arrival-time actual)]
          ;; TODO Try lazy-cat?
          ;; Concatenate the  observed stops (unmodified) and the remainder (with
          ;; estimates added). Since some of the observed stops could have been
          ;; skipped or canceled, use the last non-skipped stop as the last
          ;; observation.
          (concat actual (add-estimates rest (or (last non-skipped) last-obs))))))))


(defn trip-performance
  "Take the observed trip stop updates and the scheduled stop arrival times and
  use them to calculate how far ahead or behind schedule each stop of the trip
  was. When there is no information about a trip's arrival time at a stop,
  estimate it using either interpolation or shifting, depending on the
  information available."
  [db trip-id trip-start]
  (let [scheduled (stop-times db trip-id trip-start)
        trip-stops (->> (mc/find-maps db "trip-stops" {:trip-id trip-id
                                                       :trip-start trip-start})
                        (keep (partial add-stop-delay (comp :scheduled-arrival scheduled)))
                        (index-by :stop-sequence))]
    (add-estimates (all-stops scheduled trip-stops))))


(defn travel-times
  "Uses data stored in the database to calculate the performance of trips
  between two stops in the specified date range. See docs for a detailed
  walkthrough of the implementation and assumptions."
  [db from-stop to-stop from-dt to-dt]
  (let [from-stops (mc/find-maps db "processed-trip-stops" {:stop-id from-stop
                                                            :arrival-time {:$gte from-dt
                                                                           :$lte to-dt}})
        trip-ids (into #{} (map :trip-id) from-stops)
        to-stops (mc/find-maps db "processed-trip-stops" {:stop-id to-stop
                                                          :arrival-time {:$gte from-dt
                                                                         :$lte to-dt}
                                                          :trip-id {:$in trip-ids}})
        trip-ends (index-by (juxt :trip-id :trip-start) to-stops)
        trips (index-by :trip-id (mc/find-maps db "trips"
                                               {:trip-id {:$in trip-ids}}
                                               {:direction-id 1, :route-id 1,
                                                :trip-id 1}))]

    (->> from-stops
         (keep (fn [from-stop]
                 (when-let [to-stop (trip-ends [(:trip-id from-stop)
                                                (:trip-start from-stop)])]
                   (let [bench (- (:scheduled-arrival to-stop)
                                  ;; TODO: Use departure:
                                  (:scheduled-arrival from-stop))
                         trip (trips (:trip-id from-stop))]
                     {:dep-dt (str (:departure-time from-stop))
                      :arr-dt (str (:arrival-time to-stop))
                      :travel-time-sec (- (:arrival-time to-stop) (:departure-time
                                                                   from-stop))
                      :estimated (or (:estimated? from-stop) (:estimated? to-stop))
                      :benchmark-travel-time-sec bench
                      :route-id (:route-id trip)
                      :direction (:direction-id trip)}))))
         (sort-by :dep-dt))))


(defn dwell-times
  [db stop-id from-dt to-dt & [{:keys [route-id direction-id]}]]
  (let [stops (mc/find-maps db "trip-stops" {:arrival-time {:$gte from-dt}
                                             :departure-time {:$lte to-dt}
                                             :stop-id stop-id})]
    (map (fn [{at :arrival-time, dt :departure-time
               route :route-id, dir :direction-id}]
           {:route_id route
            :direction dir
            :arr_dt (str at)
            :dep_dt (str dt)
            :dwell_time_sec (str (- dt at))})
         stops)))

(defn trips-since
  "Find the unique trips (trip_id + trip_start_date) that have run since the
  given stamp."
  [db stamp]
  (map (comp (juxt :trip-id :trip-start) :_id)
       (mc/aggregate
        db "trip-stops"
        [{:$match {:$or [{:stamp {:$gte stamp}} {:arrival-time {:$gte stamp}}]}}
         {:$group {:_id {:trip-id "$trip-id", :trip-start "$trip-start"}}}])))

(defn recent-trips
  "Retrieves trip instances that have run in the last `ago` seconds."
  [db ago]
  (trips-since db (- (/ (System/currentTimeMillis) 1000) ago)))

(defn all-trips-for-day
  [db date-str]
  (map (fn [trip-id] [trip-id date-str])
       (mc/distinct db "trip-stops" :trip-id {:trip-start date-str})))

(defn process-trip
  [db trip-id trip-start]
  (doseq [stop (trip-performance db trip-id trip-start)]
    (let [id (str trip-id "-" trip-start "-" (:stop-id stop))]
      (mc/upsert db "processed-trip-stops" {:id id} (assoc stop :id id)))))

(defn process-trips
  "Perform post-processing on the given trip instances (trip-id, trip-start
  pairs). Retrieve the observed stops, add delays and estimates, and save back
  to the database."
  [db trip-instances]
  (doseq [[trip-id trip-start] trip-instances]
    (try
      (process-trip db trip-id trip-start)
      (info "Processing successful: " trip-id trip-start)

      (catch Exception exc
        (error exc "encountered error while processing trip:"
               [trip-id trip-start])))))

(defn post-loop
  "Start the trip-stop post-processing loop. Fill in gaps in the trip-stops
  collection and save, so that things like travel-times will be able to work
  efficiently."
  ([db updates-mult ms]
   (let [stop (chan)
         changed-trips (atom #{})
         updates (tap updates-mult (chan (sliding-buffer 100)))]
     (go-loop [changed-trips #{}, interval (timeout ms)]
       (alt!
         ;; When the timeout expires, process the changed trips and recur with an
         ;; empty set and a fresh timeout channel.
         interval (do
                    (info "Processing trips:" changed-trips)
                    (process-trips db changed-trips)
                    (recur #{} (timeout ms)))

         ;; Record changed trips:
         updates ([update]
                  (recur (conj changed-trips
                               [(:trip-id update) (:trip-start update)])
                         interval))

         ;; If the stop chan is closed, exit the loop
         stop nil))
     stop))
  ([db updates-mult]
   (post-loop db updates-mult 3600000)))


(defn- trip-info-fn
  "Returns a memoized function that retrieves information about trips."
  [db]
  (lru (fn trip-info [trip-id]
         (select-keys
          (mc/find-one-as-map db "trips" {:trip-id trip-id})
          [:direction-id :route-id]))
       :lru/threshold 500))

(defn read-updates-into-db
  "Pull trip stop updates continually and insert them into the given Mongo
  database and column. Returns a stop channel that will halt fetching of results
  and insertion when closed or given a value and a mult that can be tapped for
  trip updates."
  [db]
  (let [updates-in (chan (sliding-buffer 100) nil
                         #(error "Encountered an exception in trip update loop:" %))
        stop (trip-updates->! updates-in)
        updates-mult (async/mult updates-in)
        in (tap updates-mult (chan (sliding-buffer 100)))
        trip-info (trip-info-fn db)]
    (go-loop []
      (try
        (when-let [trip-stop (<! in)]
          (mc/upsert db "trip-stops"
                     {:id (:id trip-stop)}
                     {:$set trip-stop}))
        (catch Exception exc
          (error "Error:" exc)))
      (recur))
    [stop updates-mult]))


(defrecord TripPerformanceRecorder [interval]
  component/Lifecycle
  (start [this]
    (let [db (-> this :mongo :db)
          [stop-chan updates-mult] (read-updates-into-db db)]
      (assoc this
             :mult updates-mult
             :stop-chan stop-chan
             :post-stop-chan (post-loop db updates-mult interval)))) 

  (stop [this]
    (some-> (:stop-chan this) (async/close!))
    (some-> (:post-stop-chan this) (async/close!))
    (some-> (:mult this) (async/untap-all))
    (dissoc this :mult :stop-chan :post-stop-chan)))

(defn make-recorder [[postprocess-interval]]
  (->TripPerformanceRecorder (or postprocess-interval 600000)))
