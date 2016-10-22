(ns mbta-xtras.api-spec
  (:require [clojure.spec :as s]))

(s/def ::stop-id string?)
(s/def ::stop-sequence pos-int?)
(s/def ::trip-id string?)
(s/def ::arrival-time pos-int?)
(s/def ::trip-start (s/and string? #(re-matches #"\d{4}\d{2}\d{2}" %)))

(s/def ::stop-update
  (s/keys :req-un [::stop-id ::stop-sequence ::arrival-time ::trip-id ::trip-start]))

#_
(clojure.spec.gen/generate (s/gen ::stop-update))

(s/def ::trip-performance-request
  (s/keys :req-un [::trip-id ::trip-start]))
