(ns mbta-xtras.spec
  (:require [clojure.spec :as s]
            [clojure.spec.gen :as gen]

            [mbta-xtras.utils :refer [date-str datetime-for-stamp]]))

(s/def ::db (partial instance? com.mongodb.DB))

(s/def ::estimated? boolean?)
(s/def ::positive-int-str (s/and string? #(re-matches #"\d+")))
(s/def ::stop-id string?)
(s/def ::stop-name string?)
(s/def ::sch-arr-dt ::positive-int-str)
(s/def ::sch-dep-dt ::positive-int-str)
(s/def ::trip-id string?)
(s/def ::route-name string?)
(s/def ::trip-name string?)
(s/def ::direction-id #(re-matches #"^[01]$" %))
(s/def ::direction-name string?)
(s/def ::stop-sequence pos-int?)
(s/def ::arrival-time pos-int?)
(s/def ::departure-time pos-int?)
(s/def ::delay int?)
(s/def ::scheduled-arrival pos-int?)
(s/def ::scheduled-departure pos-int?)
(s/def ::date-str
  (s/with-gen
    (s/and string? #(re-matches #"\d{4}\d{2}\d{2}" %))
    (fn []
      (gen/fmap #(-> % (datetime-for-stamp) (date-str))
                ;; In the year 3000, we will beam to our destination using our
                ;; minds, and there will be no MBTA.
                (let [start (quot (inst-ms #inst "2000") 1000)
                      end (quot (inst-ms #inst "3000") 1000)]
                  (gen/large-integer* {:min start
                                       :max end}))))))

(s/def ::trip-start ::date-str)

(s/def ::date-trip
  (s/cat ::trip-start ::trip-id))

(s/def ::stop-update
  (s/and (s/keys :req-un [::stop-id ::stop-sequence ::arrival-time ::trip-id
                          ::trip-start] 
                 :opt-un [::delay ::departure-time ::scheduled-arrival])
         #(or (not (:departure-time %))
              (> (:departure-time %) (:arrival-time %)))))

(s/def ::scheduled-stop
  (s/keys :req-un [::stop-id ::stop-sequence ::scheduled-arrival
                   ::scheduled-departure ::trip-id ::trip-start]))

;; TODO Create a generator that generates stops in order
(s/def ::stop-updates
  (s/coll-of ::stop-update))
#_
(s/def ::stop-updates
  (s/with-gen (s/coll-of ::stop-update)
    (fn []
       )))

(s/def ::stop-description
  (s/keys :req-un [::stop-sequence ::stop-id ::stop-name ::sch-arr-dt
                   ::sch-dep-dt]))
(s/def ::stop (s/coll-of ::stop-description))
(s/def ::trip-description
  (s/keys :req-un [::route-id ::route-name ::trip-id ::trip-name ::direction-id
                   ::direction-name ::stop]))

#_
(clojure.spec.gen/generate (s/gen ::stop-updates))
