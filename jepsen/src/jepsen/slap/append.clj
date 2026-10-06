(ns jepsen.slap.append
  "Elle's list-append workload over Durable Streams. Each key is a JSON
  stream; an append is a POST of `[value]`, and a read is every message from
  the start. Transactions have one operation: the streams are independent
  objects, and strict serializability of single-operation transactions is
  linearizability of every stream."
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [jepsen [checker :as checker]
                    [client :as client]]
            [jepsen.tests.cycle.append :as append]
            [jepsen.slap.list :as list]
            [jepsen.slap.docker :refer [ip]]))

(def timeouts
  {:socket-timeout 10000, :connection-timeout 1000, :throw-exceptions false})

(def json-type {"Content-Type" "application/json"})

(defn stream-url
  [node k]
  (str "http://" (ip node) ":4437/v1/stream/append/" k))

(defn create!
  [url]
  (let [{:keys [status] :as res} (http/put url (assoc timeouts :headers json-type))]
    (when-not (#{200 201} status)
      (throw (ex-info "create failed" {:type :http, :status status, :body (:body res)})))))

(defn append!
  "Appends `v`. A 503 or a timeout is indeterminate: Durable Streams may have
  applied the append."
  [url v]
  (let [post #(http/post url (assoc timeouts :headers json-type :body (json/generate-string [v])))
        {:keys [status] :as res} (post)
        {:keys [status] :as res} (if (= 404 status) (do (create! url) (post)) res)]
    (when-not (<= 200 status 299)
      (throw (ex-info "append failed" {:type :http, :status status, :body (:body res)})))))

(defn read-all
  "Every value in the stream, following `Stream-Next-Offset` until
  `Stream-Up-To-Date`. A stream that does not exist is empty."
  [url]
  (loop [offset "-1", acc []]
    (let [{:keys [status headers body]} (http/get url (assoc timeouts :query-params {"offset" offset}))]
      (case (long status)
        404 acc
        200 (let [acc (into acc (json/parse-string body))]
              (if (get headers "stream-up-to-date")
                acc
                (recur (get headers "stream-next-offset") acc)))
        (throw (ex-info "read failed" {:type :http, :status status, :body body}))))))

(defn indeterminate?
  [e]
  (let [{:keys [type status]} (ex-data e)]
    (or (not= :http type) (= 503 status))))

(defrecord Client [node]
  client/Client
  (open! [this _test node]
    (assoc this :node node))

  (setup! [_ _test])

  (invoke! [_ _test op]
    (let [[[f k v]] (:value op)
          url       (stream-url node k)]
      (try
        (case f
          :append (do (append! url v)
                      (assoc op :type :ok))
          :r      (assoc op :type :ok, :value [[:r k (read-all url)]]))
        (catch Exception e
          (assoc op
                 :type  (if (and (= :append f) (indeterminate? e)) :info :fail)
                 :error (or (ex-data e) (.getMessage e)))))))

  (teardown! [_ _test])

  (close! [_ _test]))

(defn consistency-models
  "With leases (object-lease), every stream must be linearizable. Distributed
  placement has no leases: a node that lost a shard may serve reads from
  what it had until it learns that (Durable Streams' README), so reads may
  be stale, but nothing may be lost, duplicated or reordered."
  [opts]
  (case (:placement opts)
    :object-lease [:strict-serializable]
    :distributed  [:serializable]))

(defn workload
  [opts]
  (let [w (append/test {:key-count          (:key-count opts)
                        :min-txn-length     1
                        :max-txn-length     1
                        :max-writes-per-key (:max-writes-per-key opts)
                        :consistency-models (consistency-models opts)})]
    (let [keys (atom #{})]
      (assoc w
             :client (->Client nil)
             :generator (list/track-keys keys (:generator w))
             :final-generator (list/final-generator keys)
             :checker (checker/compose
                        {:elle (:checker w)
                         :final-reads (list/final-read-checker)})))))
