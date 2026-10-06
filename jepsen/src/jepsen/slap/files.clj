(ns jepsen.slap.files
  "A compare-and-set register per key over Slap.Files, through jepsen/node's
  files API, checked for linearizability with Knossos; deleting the file is
  writing nil. Each write picks a 1-byte body (stored inline) or a
  20,000-byte one (stored as an object), so writes switch files between the
  two. A read must return an intact body: one byte, or 20,000 of the same
  one.

  A write of a value is a PUT; of nil, a DELETE. A cas [v v'] is a GET,
  then, if the value is v, a PUT (or DELETE, for nil) with If-Match on its
  version, or with If-None-Match: * when there was no file. A cas that reads
  another value, or gets 412 or 409 (the upload expired), wrote nothing and
  fails; a 503 or a timeout on a write is indeterminate.

  At the end one client asks a node to audit the store. It stops every node
  from taking writes, waits until none is running one (a write can run on
  after its client timed out) and none of their deadlines can pass any
  more, and sweeps and reconciles everywhere; then: no file may point to a missing object, no intent may be
  left, and every object and object registration must be a file's body's
  or named by an intent. An unregistered object is the exception: a store
  request may write it after the reconciliation, and the next one would
  delete it, so it makes the result unknown; so does an audit that could
  not settle the store in time (:quiescent false), since it may see a write
  half-way."
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
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
  {:socket-timeout 15000, :connection-timeout 1000, :throw-exceptions false})

(def large 20000)

(defn file-url
  [node k]
  (str "http://" (ip node) ":4439/files/" k))

(defn body
  "The body for `v`: its digit, once or `large` times."
  [v]
  (let [digit (str v)]
    (if (< (rand/double) 0.5) digit (apply str (repeat large digit)))))

(defn value
  "The value in a body, or :corrupt if it is not one `body` makes."
  [^String b]
  (if (and (#{1 large} (count b))
           (every? #(= (first b) %) b)
           (<= (int \0) (int (first b)) (int \9)))
    (parse-long (subs b 0 1))
    :corrupt))

(defn http-error
  [what {:keys [status body]}]
  (ex-info (str what " failed") {:type :http, :status status, :body body}))

(defn fetch
  "The file's value and version, or nil."
  [url]
  (let [{:keys [status headers body] :as res} (http/get url timeouts)]
    (case (long status)
      404 nil
      200 {:value (value body), :etag (get headers "etag")}
      (throw (http-error "read" res)))))

(defn write!
  "Writes `v` (nil deletes) with `headers` as conditions. Returns true, or
  false when nothing was written (412, or 409 for an expired upload)."
  [url v headers]
  (let [{:keys [status] :as res}
        (if (nil? v)
          (http/delete url (assoc timeouts :headers headers))
          (http/put url (assoc timeouts
                               :headers (assoc headers "X-Jepsen-Write-Id"
                                               (str (java.util.UUID/randomUUID)))
                               :body (body v))))]
    (case (long status)
      204 true
      (412 409) false
      (throw (http-error "write" res)))))

(defn cas!
  [url [old new]]
  (let [current (try
                  (fetch url)
                  (catch Exception e
                    (throw (ex-info "cas read failed"
                                    (assoc (or (ex-data e) {}) :prewrite true) e))))]
    (cond
      (= :corrupt (:value current))
      (throw (ex-info "corrupt body" {:type :corrupt, :prewrite true}))
      (not= old (:value current)) false
      (and (nil? current) (nil? new)) true
      (nil? current) (write! url new {"If-None-Match" "*"})
      :else (write! url new {"If-Match" (:etag current)}))))

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
    (try
      (cond
        (= :audit (:f op))
        (let [{:keys [status body] :as res}
              (http/post (str "http://" (ip node) ":4439/audit")
                         (assoc timeouts :socket-timeout 120000))]
          (if (= 200 status)
            (assoc op :type :ok, :value (json/parse-string body true))
            (throw (http-error "audit" res))))

        (= :ready (:f op))
        (db/ready-op node op)

        :else
        (let [[k v] (:value op)
              url   (file-url node k)]
          (case (:f op)
            :read  (assoc op :type :ok, :value (independent/tuple k (:value (fetch url))))
            :write (assoc op :type (if (write! url v {}) :ok :fail))
            :cas   (assoc op :type (if (cas! url v) :ok :fail)))))
      (catch Exception e
        (assoc op
               ; A read, or a cas that failed before its write, wrote nothing.
               :type  (if (and (#{:write :cas} (:f op))
                               (not (:prewrite (ex-data e)))
                               (indeterminate? e)) :info :fail)
               :error (or (ex-data e) (.getMessage e))))))

  (teardown! [_ _test])

  (close! [_ _test]))

(defn v [] (rand/nth [nil 0 1 2 3 4]))

(defn r   [_ _] {:type :invoke, :f :read})
(defn w   [_ _] {:type :invoke, :f :write, :value (v)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(v) (v)]})

(defn corruption-checker
  "No operation may have read a corrupt body. (A read's :corrupt value also
  fails linearizability; a cas records it as an error.)"
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [bad (filter #(or (= :corrupt (get-in % [:error :type]))
                             (and (= :read (:f %)) (= :ok (:type %))
                                  (= :corrupt (second (:value %)))))
                        history)]
        {:valid? (empty? bad), :count (count bad), :examples (take 5 bad)}))))

(defn audit-checker
  "The final audit: no dangling file, no intent left, and no orphaned
  object or registration; an unreconciled object, or writes that did not
  stop, make the result unknown."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (if-let [audit (->> history (filter #(and (= :audit (:f %)) (= :ok (:type %)))) last :value)]
        {:valid?  (cond (not (:quiescent audit)) :unknown

                        (or (seq (:dangling audit))
                            (seq (:orphans audit))
                            (seq (:leaked_registrations audit))
                            (pos? (:intents audit)))
                        false

                        (seq (:unreconciled audit)) :unknown
                        (or (zero? (get-in audit [:stats :files_inline_writes] 0))
                            (zero? (get-in audit [:stats :files_object_writes] 0))) :unknown
                        :else true)
         :audit   audit}
        {:valid? :unknown, :error "no audit"}))))

(defn workload
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
     :final-generator (gen/phases (register/final-generator keys)
                                  (gen/once {:type :invoke, :f :audit}))
     :checker         (checker/compose
                        {:registers (without :audit
                                      (without :ready
                                        (independent/checker
                                          (checker/compose
                                            {:linearizable (checker/linearizable {:model (model/cas-register)})
                                             :timeline     (timeline/html)}))))
                         :corruption (corruption-checker)
                         :audit     (audit-checker)
                         :final-reads (register/final-read-checker)})}))
