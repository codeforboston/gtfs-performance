(ns mbta-xtras.utils
  (:require [clojure.spec :as s]
            [clojure.string :as str]
            [environ.core :refer [env]]
            [selmer.filters :refer [add-filter!]])
  (:import [java.time DayOfWeek LocalDate LocalDateTime
            LocalTime Instant ZonedDateTime ZoneId]
           [java.time.format DateTimeFormatter]
           [java.time.temporal ChronoUnit]))

(s/def ::local-date (partial instance? LocalDate))

;; Function specs:
(s/fdef datetime-for-stamp
        :args (s/cat :stamp number?)
        :ret ::local-date)

(s/fdef date-for-stamp
        :args (s/cat :stamp number?)
        :ret ::local-date)

(s/fdef date-strs
        :args (s/alt :default (s/cat)
                     :from-date (s/cat :date ::local-date)
                     :inc-from-date (s/cat :date ::local-date
                                           :increment integer?))
        :ret (s/coll-of string?))


;; Function definitions:
;; java.time helpers
(def default-time-zone
  (env :gtfs-zone "America/New_York"))

(defn datetime-for-stamp
  ([stamp tz]
   (-> (Instant/ofEpochSecond stamp)
       (.atZone (ZoneId/of (or tz default-time-zone)))
       (LocalDateTime/from)))
  ([stamp]
   (datetime-for-stamp stamp nil)))

(defn date-for-stamp
  [stamp & [tz]]
  (-> stamp
      (datetime-for-stamp tz)
      (.toLocalDate)))


(def date-format (DateTimeFormatter/ofPattern "yyyyMMdd"))
(defn date-for-str
  [date-str]
  (LocalDate/parse date-str date-format))

(defn local-date-for-str
  [date-str]
  (LocalDate/parse date-str date-format))

(defn local-datetime-for-str
  [date-str]
  (LocalDateTime/parse date-str date-format))

(defn local-datetime-for-str
  [date-str]
  (. (LocalDate/parse date-str date-format)
     (atStartOfDay)))

(defn datetime-for-str
  ([date-str tz]
   (ZonedDateTime/of (local-datetime-for-str date-str) (ZoneId/of tz)))
  ([date-str]
   (datetime-for-str date-str default-time-zone)))

(defn date-str [date]
  (.format date-format date))

(defn today-str []
  (.format date-format (LocalDateTime/now)))

(defn date-strs
  "Returns a lazy sequence of date strings (yyyyMMdd)."
  ([start-date increment]
   (lazy-seq (cons (date-str start-date)
                   (date-strs (. start-date plusDays increment)))))
  ([start-date]
   (date-strs start-date -1))
  ([]
   (date-strs (LocalDate/now) -1)))

(def time-format (DateTimeFormatter/ofPattern "HH:mm:ss"))

(defn time-str [time]
  (.format time time-format))

(defn time-for-str [time-str]
  (LocalTime/parse time-str time-format))


(defn set-hours
  [dt h]
  (if (> h 23)
    ;; Use plusDays and not simply plusHours, just in case there's a clock
    ;; change on the reference day.
    (-> dt
        (.plusDays (quot h 24))
        (.plusHours (rem h 24)))

    (.withHour dt h)))

(defn day-keyword [dt]
  (keyword (str/lower-case (.. dt getDayOfWeek name))))

(def day-keywords (map #(keyword (str/lower-case (.name %))) (DayOfWeek/values)))

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

(def clock-time-format
  (DateTimeFormatter/ofPattern "HH:mm:ss"))

(defn ->clock-time
  ([start-date stamp]
   (let [dt (datetime-for-stamp stamp)]
     ;; The end time may be on another day.
     ;; There are probably some DST-related issues with this.
     (if (and start-date (= (.toLocalDate dt) start-date))
       (.format clock-time-format dt)

       (str (+ 24 (.getHour dt)) ":" (.getMinute dt) ":" (.getSecond dt)))))
  ([stamp]
   (->clock-time nil stamp)))

(defn ->stamp [dt]
  (.getEpochSecond (.toInstant dt)))

;; (defn wrap-keyword-params [handler]
;;   (fn [req]
;;     (handler (update-in req [:params] (fn
;;                                        )))))

(defn index-by
  ([k coll]
   (into {} (map (juxt k identity)) coll))
  ([k]
   (map (juxt k identity))))

(defn distinct-with
  "Returns a transducer function that removes non-distinct values of (k x)."
  [k]
  (fn [rf]
    (let [seen (volatile! #{})]
      (fn
        ([] (rf))
        ([result] (rf result))
        ([result input]
         (let [v (k input)]
           (if (contains? @seen v)
                   result
                   (do (vswap! seen conj v)
                       (rf result input)))))))))

(defn ->int [x]
  (cond (string? x) (Integer/parseInt x)
        (integer? x) x))

(add-filter! :abs #(Math/abs %))
