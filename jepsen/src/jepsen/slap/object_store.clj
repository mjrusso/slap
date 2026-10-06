(ns jepsen.slap.object-store
  (:require [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [jepsen [generator :as gen]
                    [nemesis :as nemesis]]))

(def container "slap-jepsen-rustfs-1")

(defn docker!
  [action]
  (let [{:keys [exit out err]} (sh "docker" "inspect" "--format" "{{.State.Paused}}" container)
        paused? (= "true" (str/trim out))]
    (when-not (zero? exit)
      (throw (ex-info "docker inspect failed"
                      {:exit exit, :out out, :err err})))
    (when (not= paused? (= action "pause"))
      (let [{:keys [exit out err]} (sh "docker" action container)]
        (when-not (zero? exit)
          (throw (ex-info (str "docker " action " failed")
                          {:exit exit, :out out, :err err})))))
    :ok))

(defn package
  [opts]
  (when ((:faults opts) :object-store)
    {:generator (gen/delay (* 2 (:interval opts))
                          (gen/stagger (:interval opts)
                                       (gen/flip-flop
                                         (gen/repeat {:type :info, :f :pause-store})
                                         (gen/repeat {:type :info, :f :resume-store}))))
     :final-generator {:type :info, :f :resume-store}
     :nemesis (reify nemesis/Reflection
                (fs [_] #{:pause-store :resume-store})
                nemesis/Nemesis
                (setup! [this _test]
                  (docker! "unpause")
                  this)
                (invoke! [_ _test op]
                  (docker! (case (:f op)
                             :pause-store "pause"
                             :resume-store "unpause"))
                  (assoc op :value (case (:f op)
                                     :pause-store :paused
                                     :resume-store :resumed)))
                (teardown! [_ _test]
                  (docker! "unpause")))
     :perf #{{:name "object-store"
              :start #{:pause-store}
              :stop #{:resume-store}
              :color "#A7B4DA"}}}))
