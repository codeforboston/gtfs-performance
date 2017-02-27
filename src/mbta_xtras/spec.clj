(ns mbta-xtras.spec
  (:require [clojure.spec :as s]
            [clojure.spec.gen :as gen]

            [mbta-xtras.utils :refer [date-str datetime-for-stamp]]
            [clojure.string :as str]
            [mbta-xtras.spec :as spec]))

(defn increasing? [xs]
  (every? true? (map <= xs (rest xs))))

(def pos-int (gen/fmap #(Math/abs %) (gen/int)))

(def pos-int-str (gen/fmap str pos-int))

(s/def ::db (partial instance? com.mongodb.DB))

(s/def ::estimated? boolean?)
(s/def ::positive-int-str (s/with-gen
                            (s/and string? #(re-matches #"\d+" %))
                            #(pos-int-str)))
(s/def ::stamp (s/and int? pos?))
(s/def ::stop-id string?)
(s/def ::stop-name string?)
(s/def ::sch-arr-dt ::positive-int-str)
(s/def ::sch-dep-dt ::positive-int-str)
(s/def ::trip-id string?)
(s/def ::route-id string?)
(s/def ::route-name string?)
(s/def ::trip-name string?)
(s/def ::direction-id #{"0" "1"})
(s/def ::direction-name string?)
(s/def ::stop-sequence pos-int?)
(s/def ::arrival-time pos-int?)
(s/def ::departure-time pos-int?)
(s/def ::delay int?)
(s/def ::scheduled-arrival pos-int?)
(s/def ::scheduled-departure pos-int?)
(s/def ::scheduled-arrivals
  (s/with-gen
    (s/map-of ::stop-sequence ::arrival-time)
    #(gen/fmap
      (fn [[start-time stop-seq arrivals]]
        (into {} (map (fn [ss arr] [ss (+ start-time (* 10 arr))])
                      (sort stop-seq) (sort arrivals))))
      (gen/tuple
       (gen/choose 1488000000 1800000000)
       (gen/list pos-int)
       (gen/list pos-int)))))

(s/def ::date-str
  (s/with-gen
    (s/and string? #(re-matches #"\d{4}\d{2}\d{2}" %))
    (let [start (quot (inst-ms #inst "2000") 1000)
          end (quot (inst-ms #inst "3000") 1000)]
      (fn []
        (gen/fmap #(-> % (datetime-for-stamp) (date-str))
                  ;; In the year 3000, we will beam to our destination using our
                  ;; minds, and there will be no MBTA.
                  (gen/choose start end))))))

(s/def ::trip-start ::date-str)
(s/def ::time-str
  (s/with-gen
    (s/and string? #(re-matches #"\d\d:\d\d:\d\d" %))
    #(gen/fmap (fn [[h m s]] (format "%02d:%02d:%02d" h m s))
               (gen/tuple
                ;; For now, don't generate hours that go into the next day.
                (gen/choose 0 23)
                (gen/choose 0 59)
                ;; Should this be 61?
                (gen/choose 0 59)))))

(s/def ::trip-instance
  (s/cat :trip-id ::trip-id
         :trip-start ::trip-start))

(s/def ::stop-update
  (s/and (s/keys :req-un [::stop-id ::stop-sequence ::arrival-time ::trip-id
                          ::trip-start]
                 :opt-un [::delay ::departure-time ::scheduled-arrival])
         #(or (not (:departure-time %))
              (> (:departure-time %) (:arrival-time %)))))

(s/def ::scheduled-stop
  (s/keys :req-un [::stop-id ::stop-sequence ::scheduled-arrival
                   ::scheduled-departure ::trip-id ::trip-start]))

;; TODO: Shared trip-id, ordered stop-sequences, etc.
(s/def ::scheduled-stops
  (s/coll-of ::scheduled-stop))


;; TODO Create a generator that generates stops in order
(s/def ::stop-updates
  (s/coll-of ::stop-update))

(s/def ::stop-description
  (s/keys :req-un [::stop-sequence ::stop-id ::stop-name ::sch-arr-dt
                   ::sch-dep-dt]))

(s/def ::stop (s/coll-of ::stop-description))

(s/def ::trip-description
  (s/keys :req-un [::route-id ::route-name ::trip-id ::trip-name ::direction-id
                   ::direction-name ::stop]))
