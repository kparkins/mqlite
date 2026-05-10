(defproject mqlite-jepsen "0.1.0-SNAPSHOT"
  :description "Jepsen workloads for mqlite's embedded client API"
  :license {:name "MIT OR Apache-2.0"}
  :dependencies [[org.clojure/clojure "1.12.4"]
                 [jepsen "0.3.11"]]
  :main mqlite.jepsen)
