(ns mbta-xtras.trip-performance
  (:require [clojure.core.async :as async :refer [<! go-loop]]
            [environ.core :refer [env]]
            [com.stuartsierra.component :as component]
            [mbta-xtras.protobuf :refer [trip-updates->!]]
            [monger
             [collection :as mc]
             [core :as mg]])
  (:import [java.time LocalDate LocalDateTime LocalTime Instant]
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
(defn datetime-for-str [date-str] (LocalDateTime/of (LocalDate/parse date-str datetime-format)
                    LocalTime/MIDNIGHT))


(defn offset-time
  "Returns a new LocalDateTime that is offset from the reference date by the
  number of hours, minutes, and seconds given in the time-str."
  [ref-date time-str]
  (let [[h m s] (re-find #"(\d\d?):(\d\d):(\d\d)" time-str)]
    (-> ref-date
        (.truncatedTo ChronoUnit/DAYS)
        (.plusHours (long h))
        (.plusMinute (long m))
        (.plusSeconds (long s)))))


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
