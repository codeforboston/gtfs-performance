(defproject mbta-xtras "0.1.0-SNAPSHOT"
  :description "Fills in some gaps in the MBTA's API"
  :url "https://www.mbta.fyi/xapi/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha14"]
                 [org.clojure/data.json "0.2.6"]
                 [com.novemberain/monger "3.1.0"]
                 [com.stuartsierra/component "0.3.1"]
                 [environ "1.1.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/data.csv "0.1.3"]

                 [compojure "1.6.0-beta1"]
                 [aleph "0.4.2-alpha8"]

                 [com.google.protobuf/protobuf-java "2.6.1"]
                 [org.clojure/core.async "0.2.395"]
                 [org.clojure/test.check "0.9.0"]

                 [com.taoensso/timbre "4.8.0-alpha1"]
                 [selmer "1.10.5"]
                 [org.clojure/tools.nrepl "0.2.12"]]
  :java-source-paths ["src/java"]
  :main mbta-xtras.system
  :repl-options {:init-ns mbta-xtras.repl,
                 :init (set! *print-length* 50)}
  :uberjar {:aot :all})
