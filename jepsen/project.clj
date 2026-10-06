(defproject slap-jepsen "0.1.0-SNAPSHOT"
  :description "Jepsen tests for Slap streams, snapshot logs, Yjs, KV and files"
  :dependencies [[org.clojure/clojure "1.12.1"]
                 [jepsen "0.3.14"]
                 [clj-http "3.13.1"]
                 [cheshire "6.0.0"]]
  :main jepsen.slap
  :jvm-opts ["-Xmx4g" "-Djava.awt.headless=true"])
