(ns mbta-xtras.utils
  (:require [mbta-xtras.api-spec :as api]
            [clojure.spec :as s]
            [clojure.string :as str])
  (:import [java.time LocalDate LocalDateTime LocalTime Instant ZonedDateTime ZoneId]
           [java.time.format DateTimeFormatter]
           [java.time.temporal ChronoUnit]))

;; Function specs:
(s/fdef datetime-for-stamp
        :args (s/cat :stamp number?)
        :ret (partial instance? LocalDateTime))

(s/fdef datetime-for-str
        :args (s/alt :timezone (s/cat :date ::api/date-str
                                      :tz ::api/timezone)
                     :default (s/cat :date ::api/date-str))
        :ret (partial instance? ZonedDateTime))

#_
(s/fdef ->stamp
        :args (s/cat :x))

;; Function definitions:
;; java.time helpers
(defn datetime-for-stamp [stamp]
  (LocalDateTime/from (Instant/ofEpochSecond stamp)))


(def date-format (DateTimeFormatter/ofPattern "yyyyMMdd"))
(defn datetime-for-str
  ([date-str tz]
   (-> date-str
       (LocalDate/parse date-format)
       (LocalDateTime/of LocalTime/MIDNIGHT)
       (ZonedDateTime/of (ZoneId/of tz))))
  ([date-str]
   (datetime-for-str date-str "America/New_York")))

(defn date-str [date]
  (.format date-format date))

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


;; Other helpers
(defn keyfn [x]
  (str/replace (if (keyword? x) (name x) (str x))
               #"-" "_"))
