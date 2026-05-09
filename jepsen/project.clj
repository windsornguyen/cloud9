(defproject cloud9-jepsen "0.0.1-SNAPSHOT"
  :description "Jepsen tests for Cloud9 Raft"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :main cloud9.jepsen
  :dependencies [[org.clojure/clojure "1.12.4"]
                 [jepsen "0.3.11"]
                 [cheshire "6.1.0"]
                 [http-kit "2.8.1"]
                 [org.clj-commons/slingshot "0.13.0"]])
