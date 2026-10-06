(ns jepsen.slap.kv
  "A compare-and-set register per key over Slap.KV's HTTP API, checked for
  linearizability with Knossos. Each key is a row in its own partition, so
  keys spread over shards and writers. A read is a GET; a write an
  unconditional PUT; a cas [v v'] a GET, then, if the value is v, a PUT with
  If-Match on its version. A cas whose GET shows another value, or whose PUT
  gets 412, wrote nothing: it fails. A 503 or a timeout on a write is
  indeterminate (the write may still be applied)."
  (:require [clj-http.client :as http]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [independent :as independent]
                    [random :as rand]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.slap.checkers :refer [without]]
            [jepsen.slap.db :as db]
            [jepsen.slap.docker :refer [ip]]
            [jepsen.slap.register :as register]
            [knossos.model :as model]))

(def timeouts
  {:socket-timeout 10000, :connection-timeout 1000, :throw-exceptions false})

(defn row-url
  [node k]
  (str "http://" (ip node) ":4437/v1/kv/jepsen-" k "/register"))

(defn http-error
  [what {:keys [status body]}]
  (ex-info (str what " failed") {:type :http, :status status, :body body}))

(defn fetch
  "The value (a long) and version (the ETag) of the row, or nil."
  [url]
  (let [{:keys [status headers body] :as res} (http/get url timeouts)]
    (case (long status)
      404 nil
      200 (if (and (string? body) (re-matches #"[0-4]" body))
            {:value (parse-long body), :etag (get headers "etag")}
            (throw (ex-info "corrupt value" {:type :corrupt, :body body})))
      (throw (http-error "read" res)))))

(defn put!
  "Writes `v`, with `headers` as conditions. Returns true, or false on a
  failed condition (412)."
  [url v headers]
  (let [{:keys [status] :as res} (http/put url (assoc timeouts :headers headers :body (str v)))]
    (case (long status)
      204 true
      412 false
      (throw (http-error "write" res)))))

(defn indeterminate?
  [e]
  (let [{:keys [type status]} (ex-data e)]
    (or (not= :http type) (= 503 status))))

(defn cas!
  [url [old new]]
  (let [current (try
                  (fetch url)
                  (catch Exception e
                    (throw (ex-info "cas read failed"
                                    (assoc (or (ex-data e) {}) :prewrite true) e))))]
    (and current (= old (:value current))
         (put! url new {"If-Match" (:etag current)}))))

(defrecord Client [node]
  client/Client
  (open! [this _test node]
    (assoc this :node node))

  (setup! [_ _test])

  (invoke! [_ _test op]
    (try
      (if (= :ready (:f op))
        (db/ready-op node op)
        (let [[k v] (:value op)
              url   (row-url node k)]
        (case (:f op)
          :read  (assoc op :type :ok, :value (independent/tuple k (:value (fetch url))))
          :write (assoc op :type (if (put! url v {}) :ok :fail))
          :cas   (assoc op :type (if (cas! url v) :ok :fail)))))
      (catch Exception e
        (assoc op
               ; A read, or a cas that failed before its PUT, wrote nothing.
               :type  (if (and (#{:write :cas} (:f op))
                               (not (:prewrite (ex-data e)))
                               (indeterminate? e)) :info :fail)
               :error (or (ex-data e) (.getMessage e))))))

  (teardown! [_ _test])

  (close! [_ _test]))

(defn r   [_ _] {:type :invoke, :f :read})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand/long 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand/long 5) (rand/long 5)]})

(defn corruption-checker
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [bad (filter #(= :corrupt (get-in % [:error :type])) history)]
        {:valid? (empty? bad), :examples (take 5 bad)}))))

(defn workload
  "With leases (object-lease), every register must be linearizable. With
  distributed placement, reads are omitted during faults because a node
  that lost a shard may serve stale data; every client reads touched keys
  after shard ownership converges. Writes and compare-and-sets remain
  linearizable."
  [opts]
  (let [n   (count (:nodes opts))
        keys (atom #{})
        ops (case (:placement opts)
              :object-lease [r w cas cas]
              :distributed  [w cas cas])]
    {:client          (->Client nil)
     :generator       (register/track-keys
                        keys
                        (independent/concurrent-generator
                          n
                          (range)
                          (fn [_k] (gen/limit 100 (gen/mix ops)))))
     :final-generator (register/final-generator keys)
     :checker         (checker/compose
                        {:registers (without
                                      :ready
                                      (independent/checker
                                        (checker/compose
                                          {:linearizable (checker/linearizable {:model (model/cas-register)})
                                           :timeline (timeline/html)})))
                         :corruption (corruption-checker)
                         :final-reads (register/final-read-checker)})}))
