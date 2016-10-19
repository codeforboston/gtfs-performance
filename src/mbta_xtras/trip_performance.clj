(ns mbta-xtras.trip-performance
  (:require [clojure.core.async :as async :refer [<! go-loop]]
            [environ.core :refer [env]]
            [com.stuartsierra.component :as component]
            [mbta-xtras.protobuf :refer [trip-updates-chan]]
            [monger
             [collection :as mc]
             [core :as mg]]))

(defn prep-collection [db mongo-coll]
  (mc/ensure-index db mongo-coll (array-map :stop-id 1, :arrival-time 1))
  (mc/ensure-index db mongo-coll (array-map :trip-stop-id) {:unique true}))


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


(defn read-updates-into-db [db coll]
  (prep-collection db coll)
  (let [in (trip-updates-chan)]
    (go-loop []
      (let [trip-stop (<! in)]
        (mc/upsert db coll
                   {:trip-stop-id (:trip-stop-id trip-stop)}
                    {:$set trip-stop})))))


(defrecord TripPerformanceRecorder [coll]
  component/Lifecycle
  (start [this]
    (assoc this
           :record-loop (read-updates-into-db (-> this :mongo :conn) coll)))

  (stop [this]
    (when-let [c (:record-loop this)]
      (async/close! c))
    (dissoc this :record-loop)))


(defn make-recorder []
  (->TripPerformanceRecorder (env :trip-stops-collection "trip-stops")))
