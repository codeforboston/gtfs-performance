(ns mbta-xtras.api-spec
  (:require [clojure.spec :as s]))

(s/def ::float-str #(re-matches #"-?\d+(\.\d+)" %))
(s/def ::q string?)
(s/def ::lat (s/and ::float-str #(<= -90 (Float/parseFloat %) 90)))
(s/def ::lon (s/and ::float-str #(<= -180 (Float/parseFloat %) 190)))
(s/def ::stop-id string?)
(s/def ::stop-sequence pos-int?)
(s/def ::trip-id string?)
(s/def ::arrival-time pos-int?)
(s/def ::trip-start (s/and string? #(re-matches #"\d{4}\d{2}\d{2}" %)))
(s/def ::timestamp-str (s/and string? #(re-matches #"\d+")))

(s/def ::stop-update
  (s/keys :req-un [::stop-id ::stop-sequence ::arrival-time ::trip-id
                   ::trip-start]))

;; User requests:
(s/def ::q string?)
(s/def ::find-stops-query
  (s/keys :req-un [(or ::q (and ::lat ::lon) (and ::q ::lat ::lon))]))

(s/def ::trip-performance-request
  (s/keys :req-un [::trip-id ::trip-start]))

(s/def ::from-datetime ::timestamp-str)
(s/def ::to-datetime ::timestamp-str)
(s/def ::from-stop ::stop-id)
(s/def ::to-stop ::stop-id)
(s/def ::travel-times-request
  (s/keys :req-un [::from-datetime ::to-datetime ::from-stop ::to-stop]))

(s/def ::trips-for-stop-request
  (s/keys :req-un [::stop-id]))

(s/def ::trip-updates-request
  (s/keys ::req-un [::trip-id ::trip-start]))
