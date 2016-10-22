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
  ;; Not currently supported by Azure DocumentDB:
  #_
  (mc/ensure-index db mongo-coll
                   (array-map :trip-stop-id 1)
                   {:unique true}))


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

(defn trip-ids-for-date
  ([db date-str]
   (into #{} (map :trip-id)
         (mc/find-maps db "trip-stops" {:trip-start date-str})))
  ([db]
   (trip-ids-for-date db (.format datetime-format (LocalDateTime/now)))))

(defn- transduce-stop-times
  [start-date]
  (let [ref-date (datetime-for-str start-date)]
    (map (fn [stop-time]
           [(Integer/parseInt (:stop-sequence stop-time))
            (->> (:arrival-time stop-time)
                 (offset-time ref-date)
                 (.toInstant)
                 (.getEpochSecond))]))))

(defn scheduled-arrivals [db trip-id start-date]
  (into (sorted-map)
        (transduce-stop-times start-date)
        (mc/find-maps db "stop-times" {:trip-id trip-id})))

(defn trip-performance
  ""
  [db trip-id start-date]
  (let [scheduled (scheduled-arrivals db trip-id start-date)]
    (->> (mc/find-maps db "trip-stops" {:trip-id trip-id,
                                        :trip-start start-date})
         (keep (fn [stop]
                 (when-let [sched (get scheduled (:stop-sequence stop))]
                   (merge
                    (dissoc stop :_id)
                    {:delay (- (:arrival-time stop) sched)
                     :scheduled-arrival sched}))))
         (sort-by :stop-sequence))))

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
