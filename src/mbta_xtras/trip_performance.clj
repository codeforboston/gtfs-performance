(ns mbta-xtras.trip-performance
  (:require [clojure.core.async :as async :refer [<! go-loop]]
            [environ.core :refer [env]]
            [com.stuartsierra.component :as component]
            [mbta-xtras.protobuf :refer [trip-updates->!]]
            [monger
             [collection :as mc]
             [core :as mg]])
  (:import [java.time LocalDate LocalDateTime LocalTime Instant ZonedDateTime ZoneId]
           [java.time.format DateTimeFormatter]
           java.time.temporal.ChronoUnit))


(defn prep-collection [db mongo-coll]
  (mc/ensure-index db mongo-coll
                   (array-map :stop-id 1, :arrival-time 1))
  (mc/ensure-index db mongo-coll (array-map :id 1) {:unique true}))


;; java.time helpers
(defn datetime-for-stamp [stamp]
  (LocalDateTime/from (Instant/ofEpochSecond stamp)))


(def datetime-format (DateTimeFormatter/ofPattern "yyyyMMdd"))
(defn datetime-for-str
  ([date-str tz]
   (-> date-str
       (LocalDate/parse datetime-format)
       (LocalDateTime/of LocalTime/MIDNIGHT)
       (ZonedDateTime/of (ZoneId/of tz))))
  ([date-str]
   (datetime-for-str date-str "America/New_York")))

(defn set-hours
  [dt h]
  (if (> h 23)
    ;; Use plusDays and not simply plusHours, just in case there's a clock
    ;; change on the reference day.
    (-> dt
        (.plusDays (quot h 24))
        (.plusHours (rem h 24)))

    (.withHour dt h)))

;; In the manifest, trip stop times are reported as offsets from the start of
;; the day when the trip runs.
(defn offset-time
  "Returns a new LocalDateTime that is offset from the reference date by the
  number of hours, minutes, and seconds given in the time-str. The time-str has
  the format hh:mm:ss, which is roughly the wall clock time, but it can be
  bigger than 23 for trips that begin on one day and end the next day."
  [ref-date time-str]
  (let [[_ h m s] (re-find #"(\d\d?):(\d\d):(\d\d)" time-str)]
    (-> ref-date
        (.truncatedTo ChronoUnit/DAYS)
        (set-hours (Long/parseLong h))
        (.withMinute (Long/parseLong m))
        (.withSecond (Long/parseLong s)))))

(defn ->stamp [dt]
  (.getEpochSecond (.toInstant dt)))

(defn trip-ids-for-date
  ([db date-str]
   (into #{} (map :trip-id)
         (mc/find-maps db "trip-stops" {:trip-start date-str})))
  ([db]
   (trip-ids-for-date db (.format datetime-format (LocalDateTime/now)))))

(defn- transduce-stop-times
  [start-date]
  (let [ref-date (datetime-for-str start-date)]
    (map (fn [{at :arrival-time, ss :stop-sequence, :as stop-time}]
           [(Integer/parseInt ss)
            (-> (dissoc stop-time :_id)
                (assoc :stop-sequence (Integer/parseInt ss)
                       :scheduled-arrival (->stamp (offset-time ref-date at))
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

(defn add-estimates [all-stops & [last-obs]]
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

(defn travel-times [db from-stop to-stop from-dt to-dt]
  )

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
