(ns jepsen.slap.yjs
  "An array of unique integers in one Yjs document, through jepsen/node's
  Yjs API. An add is acknowledged once the document's server on that node
  has stored it; a read loads the document from storage. Every acknowledged
  add must be in the final reads and, with leases, in every read that starts
  after it is acknowledged."
  (:require [cheshire.core :as json]
            [clojure.set :as set]
            [clj-http.client :as http]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [jepsen.history :as h]
            [jepsen.slap.docker :refer [ip]]))

(def timeouts
  {:socket-timeout 15000, :connection-timeout 1000, :throw-exceptions false})

(defn doc-url
  [node]
  (str "http://" (ip node) ":4438/yjs/set"))

(defrecord Client [node]
  client/Client
  (open! [this _test node]
    (assoc this :node node))

  (setup! [_ _test])

  (invoke! [_ _test op]
    (try
      (case (:f op)
        :stats
        (let [{:keys [status body]} (http/get (str "http://" (ip node) ":4438/stats") timeouts)]
          (if (= 200 status)
            (assoc op :type :ok, :value (json/parse-string body true))
            (assoc op :type :fail, :error [status body])))

        :add
        (let [{:keys [status body]} (http/post (doc-url node) (assoc timeouts :body (str (:value op))))]
          (case (long status)
            200 (assoc op :type :ok)
            ; Not sent to the document's server.
            503 (assoc op :type :fail, :error body)
            (assoc op :type :info, :error [status body])))

        :read
        (let [{:keys [status body]} (http/get (doc-url node) timeouts)]
          (if (= 200 status)
            (assoc op :type :ok, :value (mapv parse-long (json/parse-string body)))
            (assoc op :type :fail, :error [status body]))))
      (catch java.net.SocketTimeoutException _
        (assoc op :type (if (= :read (:f op)) :fail :info), :error :timeout))
      (catch java.net.ConnectException _
        (assoc op :type :fail, :error :connection-refused))))

  (teardown! [_ _test])

  (close! [_ _test]))

(defn generator
  "Adds of 0, 1, 2, ..., with a read one time in four."
  []
  (let [next-value (atom -1)]
    (fn []
      (if (< (rand) 0.25)
        {:f :read}
        {:f :add, :value (swap! next-value inc)}))))

(defn compaction-checker
  "Documents were compacted during the run (the nodes compact every 1 KiB
  of updates); a run that compacted none did not test snapshots, and is
  unknown."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (if-let [stats (->> history (filter #(and (= :stats (:f %)) (= :ok (:type %)))) last :value)]
        {:valid? (if (pos? (:yjs_compactions stats 0)) true :unknown)
         :stats  stats}
        {:valid? :unknown, :error "no stats"}))))

(defn set-checker
  [opts]
  (let [set-full (checker/set-full {:linearizable? (= :object-lease (:placement opts))})]
    (reify checker/Checker
      (check [_ test history checker-opts]
        (checker/check set-full test
                       (h/map (fn [op]
                                (if (and (= :read (:f op)) (= :ok (:type op)))
                                  (update op :value set)
                                  op))
                              (h/remove #(= :stats (:f %)) history))
                       checker-opts)))))

(defn array-checker
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [adds (set (for [op history
                            :when (and (= :add (:f op)) (= :invoke (:type op)))]
                        (:value op)))
            acknowledged (set (for [op history
                                   :when (and (= :add (:f op)) (= :ok (:type op)))]
                               (:value op)))
            reads (filter #(and (= :read (:f %)) (= :ok (:type %))) history)
            finals (filter :final reads)
            final-values (map :value finals)
            expected (count (:nodes test))
            scheduled (frequencies (map :process (filter #(and (:final %) (= :invoke (:type %))) history)))
            completed (frequencies (map :process finals))
            reference (first final-values)
            positions (zipmap reference (range))
            malformed (filter (fn [op]
                                (let [value (:value op)]
                                  (or (not (vector? value))
                                      (not= (count value) (count (set value)))
                                      (not (set/subset? (set value) adds)))))
                              reads)
            order-drift (when (seq final-values)
                          (filter (fn [op]
                                    (let [value (:value op)]
                                      (or (not (set/subset? (set value) (set reference)))
                                          (not (apply < (map positions value))))))
                                  reads))
            lost (set/difference acknowledged (set reference))]
        {:valid? (cond
                   (seq malformed) false
                   (and (seq final-values) (seq lost)) false
                   (seq order-drift) false
                   (and (seq final-values) (not (apply = final-values))) false
                   (or (empty? adds)
                       (not= expected (count scheduled))
                       (not (every? #(= 1 %) (vals scheduled)))
                       (not= scheduled completed)) :unknown
                   :else true)
         :malformed (take 5 malformed)
         :order-drift (take 5 order-drift)
         :lost (sort lost)
         :scheduled scheduled
         :completed completed}))))

(defn workload
  [opts]
  {:client          (->Client nil)
   :generator       (generator)
   :final-generator (gen/phases (gen/each-thread {:f :read, :final true})
                                (gen/once {:f :stats}))
   :checker         (checker/compose
                      {:set         (set-checker opts)
                       :array       (array-checker)
                       :compactions (compaction-checker)})})
