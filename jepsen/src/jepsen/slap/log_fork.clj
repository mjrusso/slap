(ns jepsen.slap.log-fork
  "Snapshot-log and document forks checked against recorded source entries."
  (:require [clojure.string :as str]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.slap.db :as db]
            [jepsen.slap.docker :refer [ip]]))

(def ^:dynamic *deadline* nil)

(defn request! [node mode request]
  (let [remaining (if *deadline* (max 1 (quot (- *deadline* (System/nanoTime)) 1000000)) 30000)
        port (if (= mode :yjs) 4438 4440)
        response (http/post (str "http://" (ip node) ":" port "/fork/operation")
                            {:body (json/generate-string request), :content-type :json,
                             :socket-timeout (min 30000 remaining), :connection-timeout (min 1000 remaining),
                             :throw-exceptions false})
        body (try (json/parse-string (:body response) true) (catch Exception _ nil))]
    (cond
      (and (= 200 (:status response)) (map? body)) body
      (or (= 500 (:status response)) (= 200 (:status response)))
      (throw (ex-info "invalid branch response" {:status (:status response), :body body, :corrupt true}))
      :else (throw (ex-info "branch operation failed" {:status (:status response), :body body,
                                ; A busy log or fork source is still being created.
                                :retryable (= "busy" (get-in body [:error :code]))})))))

(defn script [mode id [a b c] snapshot?]
  (let [n (Long/parseLong (last (str/split id #"-")))
        x (+ (* n 10) 1) y (+ (* n 10) 2) z (+ (* n 10) 3) own (+ (* n 10) 4)
        operations (concat
                     [{:action "initialize", :key a}
                      {:action "append", :key a, :value x}
                      {:action "append", :key a, :value y}]
                     (when (= mode :yjs) [{:action "remove", :key a, :value x}])
                     (when snapshot? (concat
                       (for [value [(+ (* n 10) 6) (+ (* n 10) 7) (+ (* n 10) 8)]]
                         {:action "append", :key a, :value value})
                       [(cond-> {:action "compact", :key a}
                          (= mode :yjs) (assoc :extra (+ (* n 10) 9)))]))
                     [ (cond-> {:action "fork", :source a, :key b}
                        (not snapshot?) (assoc :concurrent {:action "append", :key a, :value (* n 10), :id (str id "/concurrent")}))
                      {:action "append", :key a, :value z}
                      {:action "append", :key b, :value own}
                      {:action "read", :key a}
                      {:action "read", :key b}
                      {:action "fork", :source b, :key c}
                      {:action "compact", :key b}
                      {:action "delete", :key a}
                      {:action "fork", :source a, :key b, :retry true}
                      {:action "delete", :key b}
                      {:action "read", :key c, :retained true}]
                     (when (= mode :yjs) [{:action "join", :key c}])
                     [{:action "append", :key c, :value (+ (* n 10) 5)}
                      {:action "compact", :key c}
                      {:action "read", :key c, :retained true}])]
    (mapv (fn [n op] (assoc op :id (str id "/" n))) (range) operations)))

(defn scenario! [state node mode actor]
  (let [scenario (get-in @state [:scenarios (get-in @state [:actors actor])])]
    (if (and scenario (< (:stage scenario) (count (:script scenario)))) scenario
      (let [n (:next-id (swap! state update :next-id inc))
            id (str actor "-" n)
            plan (request! node mode {:action "plan", :id id})
            scenario {:id id, :keys (:keys plan), :shards (:shards plan), :stage 0,
                      :script (script mode id (:keys plan) (even? n)), :completed [], :representations {}}]
        (swap! state #(-> % (assoc-in [:actors actor] id) (assoc-in [:scenarios id] scenario)))
        scenario))))

(defn client-context [scenario key]
  (let [previous (last (get-in scenario [:representations key]))
        n (Long/parseLong (last (str/split (:id scenario) #"-")))]
    {:state (:state previous), :at (:offset previous 0), :client (+ 1 (* n 4) (.indexOf (:keys scenario) key))}))

(defn remember [scenario mode request reply]
  (let [scenario (if-let [concurrent (:concurrent reply)]
                   (update-in scenario [:representations (get-in request [:concurrent :key])] (fnil conj []) concurrent)
                   scenario)
        versions (get-in scenario [:representations (:source request)])
        captured (last (filter #(= (:offset reply) (:offset %)) versions))
        representation (if (= "fork" (:action request)) captured (select-keys reply [:offset :state]))]
    (cond-> scenario
      (and (= mode :yjs) (#{"initialize" "append" "remove" "compact" "fork"} (:action request)) (not (:retry request)))
      (update-in [:representations (:key request)] (fnil conj []) representation))))

(defn step! [state node mode scenario]
  (let [raw (nth (:script scenario) (:stage scenario))
        request (cond-> raw
                  (and (= mode :yjs) (#{"append" "remove" "compact"} (:action raw)))
                  (merge (client-context scenario (:key raw)))
                  (and (= mode :yjs) (:concurrent raw))
                  (update :concurrent merge (client-context scenario (get-in raw [:concurrent :key]))))
        reply (try (request! node mode request)
                   (catch Exception e
                     (when (and (= 409 (:status (ex-data e))) (not (:retryable (ex-data e)))
                                (= "fork" (:action request)) (not (:retry request)))
                       (swap! state update-in [:scenarios (:id scenario)]
                              #(-> % (assoc :stage (count (:script %))) (assoc :rejected request)
                                     (cond-> (get-in (ex-data e) [:body :error :concurrent])
                                       (update :completed conj {:request (:concurrent request), :reply (get-in (ex-data e) [:body :error :concurrent])})))))
                     (throw e)))
        record {:request request, :reply reply, :plan (select-keys scenario [:keys :shards])}]
    (swap! state update-in [:scenarios (:id scenario)]
           #(-> % (update :stage inc) (update :completed conj record) (remember mode request reply)))
    record))

(defn live-keys [state]
  (reduce (fn [keys {:keys [request]}]
            (case (:action request)
              ("initialize" "fork") (if (:retry request) keys (conj keys (:key request)))
              "delete" (disj keys (:key request))
              keys)) #{} (mapcat :completed (vals (:scenarios state)))))

(defn retry-request! [f]
  (loop []
    (when (and *deadline* (>= (System/nanoTime) *deadline*))
      (throw (ex-info "branch recovery deadline" {})))
    (let [result (try {:ok (f)}
                      (catch Exception e
                        (if (:corrupt (ex-data e)) (throw e) {:retry true})))]
      (if (:retry result) (recur) (:ok result)))))

; Each attempt reads the scenario again: a rejected fork ends it. Finished
; steps go to `done` at once, so they reach the history if resolution times out.
(defn resolve! [state node mode done]
  (db/await-ready node)
  (binding [*deadline* (+ (System/nanoTime) 180000000000)]
    (doseq [id (keys (:scenarios @state))]
      (loop []
        (when-let [record (retry-request! #(let [scenario (get-in @state [:scenarios id])]
                                              (when (< (:stage scenario) (count (:script scenario)))
                                                (step! state node mode scenario))))]
          (swap! done conj record)
          (recur))))))

(defrecord Client [node mode state]
  client/Client
  (open! [this _test node] (assoc this :node node))
  (setup! [_ _test])
  (invoke! [_ test op]
    (let [request (atom nil)]
      (try
        (case (:f op)
          :branch-step
          (let [scenario (scenario! state node mode (mod (:process op) (:concurrency test 5)))]
            (reset! request (nth (:script scenario) (:stage scenario)))
            (assoc op :type :ok, :value (step! state node mode scenario)))
          :branch-resolve
          (let [done (atom [])]
            (try (resolve! state node mode done)
                 (assoc op :type :ok, :value @done)
                 (catch Exception e
                   (when (:corrupt (ex-data e)) (throw e))
                   (assoc op :type :info, :value @done, :error (or (ex-data e) (.getMessage e))))))
          :branch-recover
          (binding [*deadline* (+ (System/nanoTime) 180000000000)]
          (assoc op :type :ok, :node node, :value
                 (mapv (fn [key] {:request {:action "read", :key key, :final true},
                                 :reply (retry-request! #(request! node mode {:action "recover", :key key}))})
                       (live-keys @state))))
          :branch-audit
          (binding [*deadline* (+ (System/nanoTime) 180000000000)]
          (assoc op :type :ok, :value
                 (retry-request! #(request! node mode {:action "audit", :keys (vec (mapcat :keys (vals (:scenarios @state)))),
                                            :rejected (vec (keep :rejected (vals (:scenarios @state))))})))))
        (catch Exception e
          (assoc op :type (if (or (:corrupt (ex-data e))
                                 (and (= 409 (:status (ex-data e))) (not (:retryable (ex-data e))))) :fail :info),
                    :value (cond-> {:request @request}
                             (and (= 409 (:status (ex-data e))) (get-in (ex-data e) [:body :error :concurrent]))
                             (assoc :concurrent-record {:request (:concurrent @request), :reply (get-in (ex-data e) [:body :error :concurrent])})), :error (or (ex-data e) (.getMessage e)))))))
  (teardown! [_ _test])
  (close! [_ _test]))

(defn records [history]
  (mapcat (fn [op]
            (cond
              (get-in op [:value :concurrent-record]) [(get-in op [:value :concurrent-record])]
              (and (= :branch-resolve (:f op)) (#{:ok :info} (:type op))) (:value op)
              (= :ok (:type op)) (case (:f op)
                                   :branch-step [(:value op)]
                                   :branch-recover (:value op)
                                   [])
              :else [])) history))

(defn version [stream offset]
  (last (filter #(= offset (:offset %)) (:versions stream))))

(declare apply-record)

(defn concurrent-record [placement world {:keys [request reply]}]
  (let [append {:request (:concurrent request), :reply (:concurrent reply)}
        capture {:request (dissoc request :concurrent), :reply (dissoc reply :concurrent)}
        order (if (= (:offset reply) (get-in reply [:concurrent :offset])) [append capture] [capture append])]
    (reduce (partial apply-record placement) world order)))

(defn apply-record [placement world {:keys [request reply] :as record}]
  (if (:concurrent request) (concurrent-record placement world record)
  (let [{:keys [action key source value]} request
        stream (get world key)
        latest (last (:versions stream))
        append-version (fn [values] (update stream :versions conj {:offset (:offset reply), :values values}))]
    (case action
      "initialize" (if stream world (assoc world key {:versions [{:offset 0 :values []}], :snapshot-offset nil, :deleted false}))
      "append" (if-not (and stream (not (:deleted stream)) (> (:offset reply -1) (:offset latest)))
                   (throw (ex-info "invalid append offset" {:request request :reply reply}))
                   (assoc world key (append-version (conj (:values latest) value))))
      "remove" (assoc world key (append-version (filterv #(not= value %) (:values latest))))
      "compact" (if (version stream (:offset reply))
                  (let [stream (assoc stream :snapshot-offset (:offset reply))
                        stream (if (and (:extra request) (not (some #{(:extra request)} (:values latest))))
                                 (update stream :versions conj (update latest :values conj (:extra request))) stream)]
                    (assoc world key stream))
                      (throw (ex-info "invalid snapshot offset" {:request request :reply reply})))
      "fork"
      (if stream
        (if (and (not (:deleted stream)) (= source (:source stream)) (= reply (:capture stream))) world
          (throw (ex-info "changed retry capture" {:request request :reply reply :stream stream})))
        (let [source-stream (get world source)
              captured (version source-stream (:offset reply))]
          (when-not (and captured (not (:deleted source-stream))
                         (or (= :distributed placement) (= captured (last (:versions source-stream))))
                         (= (:snapshot_offset reply) (:snapshot-offset source-stream))
                         (or (nil? (:snapshot_offset reply))
                             (<= (:snapshot_offset reply) (:offset reply))))
            (throw (ex-info "invalid capture" {:request request :reply reply :source source-stream})))
          (assoc world key {:versions [captured], :source source, :capture reply, :snapshot-offset (:snapshot_offset reply), :deleted false})))
      "delete" (assoc-in world [key :deleted] true)
      ("read" "join")
      (let [allowed (if (or (:final request) (= placement :object-lease)) [latest] (:versions stream))]
        (when-not (and stream (not (:deleted stream))
                       (some #(and (= (:values reply) (:values %))
                                   (or (= action "join") (= (:offset reply) (:offset %)))) allowed))
          (throw (ex-info "invalid branch reconstruction" {:request request :reply reply :expected allowed})))
        world)
      world))))

(defn validate-records [placement records]
  (try
    (reduce (partial apply-record placement) {} records)
    []
    (catch clojure.lang.ExceptionInfo e [(assoc (ex-data e) :error (.getMessage e))])))

(defn observed-coverage [records]
  (:coverage
    (reduce
      (fn [{:keys [parents deleted forked] :as state} {:keys [request reply plan]}]
        (let [{:keys [action key source]} request
              ancestors (take-while some? (rest (iterate parents key)))
              bump #(update-in %1 [:coverage %2] inc)]
          (case action
            "fork" (if (:retry request) state
                     (cond-> (-> state (assoc-in [:parents key] source) (update :forked conj source) (bump :forks))
                       (some? (:snapshot_offset reply)) (bump :snapshot-captures)
                       (nil? (:snapshot_offset reply)) (bump :unsnapshotted-captures)
                       (:concurrent request) (bump :concurrent-captures)
                       (parents source) (bump :chains)
                       (and (= 3 (count (:shards plan))) (not= (first (:shards plan)) (second (:shards plan))) (not= (second (:shards plan)) (nth (:shards plan) 2))) (bump :cross-shard)))
            "append" (cond-> state (parents key) (bump :target-appends) (forked key) (bump :source-appends))
            "delete" (update state :deleted conj key)
            "compact" (bump state :compactions)
            "join" (bump state :joins)
            "read" (cond-> state
                     (and (:retained request) (seq ancestors) (every? deleted ancestors)) (bump :retained-reads)
                     (:final request) (bump :final-reads))
            state)))
      {:parents {} :deleted #{} :forked #{}
       :coverage (zipmap [:forks :snapshot-captures :unsnapshotted-captures :concurrent-captures
                         :chains :cross-shard :target-appends :source-appends :retained-reads
                         :compactions :joins :final-reads] (repeat 0))}
      records)))

(defn branch-checker
  ([placement] (branch-checker placement :log))
  ([placement mode]
  (reify checker/Checker
    (check [_ test history _opts]
      (let [records (vec (records history))
            errors (validate-records placement records)
            final-ops (filter #(and (= :branch-recover (:f %)) (= :ok (:type %))) history)
            audit (last (filter #(and (= :branch-audit (:f %)) (= :ok (:type %))) history))
            unexpected (filter #(or (get-in % [:error :corrupt]) (and (= :fail (:type %))
                                      (or (not= "fork" (get-in % [:value :request :action]))
                                          (get-in % [:value :request :retry])))) history)
            coverage (cond-> (observed-coverage records) (= mode :log) (dissoc :joins))]
        {:valid? (cond (or (seq errors) (seq unexpected)) false
                       (or (not (every? pos? (vals coverage)))
                           (not= (set (:nodes test)) (set (map :node final-ops)))
                           (not= 0 (get-in audit [:value :rows]))) :unknown
                       :else true)
         :errors errors :coverage coverage})))))

(defn workload [mode opts]
  (let [state (atom {:next-id -1, :actors {}, :scenarios {}})]
    {:client (->Client nil mode state)
     :generator (fn [] {:f :branch-step})
     :final-generator (gen/phases (gen/once {:f :branch-resolve})
                                  (gen/each-thread {:f :branch-recover})
                                  (gen/once {:f :branch-audit}))
     :checker (branch-checker (:placement opts) mode)}))
