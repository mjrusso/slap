(ns jepsen.slap.snapshot-log
  "Elle's list-append workload over Slap.SnapshotLog, through jepsen/node's
  log API. Each key is a log whose entries are integers; an append is an
  append, and a read rebuilds the list from the current snapshot and the
  entries after it, as a new process does. Unlike Yjs updates, these
  entries cannot be applied twice or out of order, so an entry lost,
  duplicated or reordered by a snapshot, a trim or a reset shows.

  Each node starts followers for keys served by connected nodes, applies
  what it reads, and publishes a snapshot of its list every 5 entries.
  Followers on different nodes may publish snapshots for the same log.
  After faults heal, one client restores followers for every touched key on
  every node, then checks that each caught up to its log. The check reports snapshots
  published and superseded, and followers reset by another node's trim; a
  run without a snapshot, or without competition or a reset, is unknown."
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [jepsen.tests.cycle.append :as append]
            [jepsen.slap.checkers :refer [without]]
            [jepsen.slap.list :as list]
            [jepsen.slap.docker :refer [ip]]))

(def timeouts
  {:socket-timeout 15000, :connection-timeout 1000, :throw-exceptions false})

(defn log-url
  [node k]
  (str "http://" (ip node) ":4440/logs/" k))

(defrecord Client [node]
  client/Client
  (open! [this _test node]
    (assoc this :node node))

  (setup! [_ _test])

  (invoke! [_ _test op]
    (try
      (if (#{:check :restore} (:f op))
        (let [f (:f op)
              {:keys [status body]} (http/post (str "http://" (ip node) ":4440/" (name f))
                                               (assoc timeouts
                                                      :socket-timeout (if (= f :check) 720000 180000)
                                                      :body (json/generate-string (:value op))))]
          (if (= 200 status)
            (assoc op :type :ok, :value (if (= f :check)
                                         (json/parse-string body true)
                                         (:value op)))
            (assoc op :type :fail, :error [status body])))
        (let [[[f k v]] (:value op)
              url       (log-url node k)]
          (case f
            :append (let [{:keys [status body]} (http/post url (assoc timeouts :body (str v)))]
                      (if (= 200 status)
                        (assoc op :type :ok)
                        ; The append may still be stored.
                        (assoc op :type :info, :error [status body])))
            :r      (let [{:keys [status body]} (http/get url timeouts)]
                      (if (= 200 status)
                        (assoc op :type :ok, :value [[:r k (json/parse-string body)]])
                        (assoc op :type :fail, :error [status body]))))))
      (catch java.net.SocketTimeoutException _
        (assoc op :type (if (= :r (ffirst (:value op))) :fail :info), :error :timeout))
      (catch java.net.ConnectException _
        (assoc op :type :fail, :error :connection-refused))))

  (teardown! [_ _test])

  (close! [_ _test]))

(defn followers-checker
  "Every required follower caught up with its log. A run needs a published
  snapshot and evidence of competition or a reset to cover those paths."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (if-let [result (->> history (filter #(and (= :check (:f %)) (= :ok (:type %)))) last :value)]
        {:valid? (cond (seq (:mismatches result)) false
                       (not (some #(and (= :restore (:f %)) (= :ok (:type %))) history)) :unknown
                       (or (seq (:read_errors result))
                           (seq (:unreachable result))
                           (seq (:missing result))
                           (seq (:lagging result))
                           (zero? (:keys result 0))
                           (not= (* (:nodes result 0) (:keys result 0)) (:followers result))
                           (zero? (get-in result [:stats :log_snapshots] 0))
                           (and (zero? (get-in result [:stats :log_superseded] 0))
                                (zero? (get-in result [:stats :log_resets] 0))
                                (not (some #(>= (count %) 2)
                                           (vals (:publication_nodes result)))))) :unknown
                       :else true)
         :check  result}
        {:valid? :unknown, :error "no check"}))))

(defn workload
  "Strict serializability of single-append transactions is linearizability
  of every log, with leases; without (distributed), reads may be stale, and
  nothing may be lost, duplicated or reordered."
  [opts]
  (let [w (append/test {:key-count          (:key-count opts)
                        :min-txn-length     1
                        :max-txn-length     1
                        :max-writes-per-key (:max-writes-per-key opts)
                        :consistency-models (case (:placement opts)
                                              :object-lease [:strict-serializable]
                                              :distributed  [:serializable])})
        keys (atom #{})]
    {:client          (->Client nil)
     :generator       (list/track-keys keys (:generator w))
     :final-generator (gen/phases
                        (gen/once (fn [_ _] {:type :invoke, :f :restore, :value (sort @keys)}))
                        (gen/once (fn [_ _] {:type :invoke, :f :check, :value (sort @keys)}))
                        (list/final-generator keys))
     :checker         (checker/compose
                        {:elle      (without :restore (without :check (:checker w)))
                         :followers (followers-checker)
                         :final-reads (list/final-read-checker)})}))
