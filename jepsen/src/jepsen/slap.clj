(ns jepsen.slap
  "Jepsen tests for Durable Streams (Elle list-append over streams),
  slap_yjs (a set in a Yjs document), slap_kv and slap_files
  (compare-and-set registers), on five nodes in Docker (see docker/compose.yml and the
  README)."
  (:require [clojure.string :as str]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [net :as net]
                    [os :as os]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.nemesis.combined :as nc]
            [jepsen.slap [append :as append]
                         [db :as db]
                         [docker :as docker]
                         [files :as files]
                         [kv :as kv]
                         [object-store :as object-store]
                         [snapshot-log :as snapshot-log]
                         [yjs :as yjs]]))

(def workloads
  {:append append/workload
   :yjs    yjs/workload
   :kv     kv/workload
   :files  files/workload
   :snapshot-log snapshot-log/workload})

(def default-faults
  "Clock faults are left out: containers share the host's clock."
  #{:partition :kill :pause})

(def all-faults (conj default-faults :object-store))

(defn parse-faults
  [s]
  (if (= "none" s)
    #{}
    (set (map keyword (str/split s #",")))))

(defn nemesis-package
  "Partitions, and kills and pauses of nodes. (nc/nemesis-package would also
  set up file corruption, whatever the faults.)"
  [opts]
  (nc/compose-packages
    (remove nil? [(nc/partition-package opts)
                  (nc/db-package opts)
                  (object-store/package opts)])))

(def fault-events
  {:partition {:start :start-partition, :stop :stop-partition}
   :kill {:start :kill, :stop :start}
   :pause {:start :pause, :stop :resume}
   :object-store {:start :pause-store, :stop :resume-store}})

(defn completed-fault?
  [fault op]
  (and (= :nemesis (:process op))
       (= :info (:type op))
       (= (get-in fault-events [fault :start]) (:f op))
       (case fault
         :partition (and (vector? (:value op))
                         (= :isolated (first (:value op)))
                         (seq (second (:value op))))
         :kill (and (map? (:value op)) (seq (:value op))
                    (every? nil? (vals (:value op))))
         :pause (and (map? (:value op)) (seq (:value op))
                     (every? #{:signaled} (vals (:value op))))
         :object-store (= :paused (:value op))
         false)))

(defn completed-recovery?
  [fault op]
  (and (= :nemesis (:process op))
       (= :info (:type op))
       (= (get-in fault-events [fault :stop]) (:f op))
       (case fault
         :partition (= :network-healed (:value op))
         :kill (and (map? (:value op)) (seq (:value op))
                    (every? string? (vals (:value op))))
         :pause (and (map? (:value op)) (seq (:value op))
                     (every? #{:signaled} (vals (:value op))))
         :object-store (= :resumed (:value op))
         false)))

(defn fault-checker
  [faults]
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [missing (for [fault faults
                          :let [starts (filter #(completed-fault? fault %) history)
                                stops (filter #(completed-recovery? fault %) history)]
                          :when (not (some (fn [start]
                                             (some #(> (:index %) (:index start)) stops))
                                           starts))]
                      fault)]
        {:valid? (if (empty? missing) true :unknown)
         :missing (vec missing)}))))

(defn slap-test
  [opts]
  (let [workload ((workloads (:workload opts)) opts)
        db       (db/db)
        nemesis  (nemesis-package
                   {:db        db
                    :nodes     (:nodes opts)
                    :faults    (:faults opts)
                    :interval  (:nemesis-interval opts)
                    :partition {:targets [:one :majority :majorities-ring]}
                    :kill      {:targets [:one :minority]}
                    :pause     {:targets [:one :minority]}})]
    (merge tests/noop-test
           opts
           {:name         (str "slap " (name (:workload opts)) " "
                               (name (:placement opts)) " "
                               (if (seq (:faults opts))
                                 (str/join "," (map name (sort (:faults opts))))
                                 "no-faults"))
            :store-prefix (str "run-" (System/currentTimeMillis))
            :os           os/noop
            :db           db
            :remote       docker/remote
            :net          net/iptables
            :client       (:client workload)
            :nemesis      (:nemesis nemesis)
            :checker      (checker/compose
                            {:perf       (checker/perf {:nemeses (:perf nemesis)})
                             :stats      (checker/stats)
                             :exceptions (checker/unhandled-exceptions)
                             :timeline   (timeline/html)
                             :faults     (fault-checker (:faults opts))
                             :workload   (:checker workload)})
            :generator    (gen/phases
                            (->> (:generator workload)
                                 (gen/stagger (/ (:rate opts)))
                                 (gen/nemesis (:generator nemesis))
                                 (gen/time-limit (:time-limit opts)))
                            (gen/log "Healing the cluster")
                            (gen/nemesis (:final-generator nemesis))
                            (gen/log "Waiting for recovery")
                            (gen/sleep (:recovery-time opts))
                            (gen/clients (:final-generator workload)))})))

(def opt-spec
  [[nil "--workload NAME" "append, yjs, kv, files or snapshot-log"
    :default  :append
    :parse-fn keyword
    :validate [workloads (str "one of " (str/join ", " (map name (keys workloads))))]]
   [nil "--nemesis FAULTS" "Comma-separated faults (partition, kill, pause, object-store), or none"
    :id       :faults
    :default  default-faults
    :parse-fn parse-faults
    :validate [#(every? all-faults %) (str "faults are " (str/join ", " (map name all-faults)))]]
   [nil "--nemesis-interval SECONDS" "Mean seconds between faults"
    :default  10
    :parse-fn parse-long]
   [nil "--recovery-time SECONDS" "Seconds to wait after healing, before final reads"
    :default  20
    :parse-fn parse-long]
   [nil "--rate HZ" "Operations per second"
    :default  50
    :parse-fn parse-double]
   [nil "--placement STRATEGY" "object-lease or distributed"
    :default  :object-lease
    :parse-fn keyword
    :validate [#{:object-lease :distributed} "object-lease or distributed"]]
   [nil "--lease-ttl MS" "Lease TTL with object-lease"
    :default  6000
    :parse-fn parse-long]
   [nil "--shards N" "Shards in the cluster"
    :default  8
    :parse-fn parse-long]
   [nil "--key-count N" "Streams in use at once (append)"
    :default  10
    :parse-fn parse-long]
   [nil "--max-writes-per-key N" "Appends per stream before it is retired (append)"
    :default  100
    :parse-fn parse-long]])

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  slap-test
                                         :opt-spec opt-spec})
                   (cli/serve-cmd))
            args))
