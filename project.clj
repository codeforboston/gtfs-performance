(defproject mbta-xtras "0.1.0-SNAPSHOT"
  :description "Fills in some gaps in the MBTA's API"
  :url "https://www.mbta.fyi/xapi/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha13"]
                 [org.clojure/data.json "0.2.6"]
                 [com.novemberain/monger "3.1.0"]
                 [com.stuartsierra/component "0.3.1"]
                 [environ "1.1.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/data.csv "0.1.3"]

                 [compojure "1.6.0-beta1"]
                 [aleph "0.4.2-alpha8"]]
  :main mbta-xtras.system
  :uberjar {:aot :all})
