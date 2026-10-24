(ns jepsen.slap.fork
  "Cross-shard creation retries, reference retention and independent copies."
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [clj-http.client :as http]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.slap.db :as db]
            [jepsen.slap.docker :refer [ip]]))

(def timeouts {:socket-timeout 35000, :connection-timeout 1000, :throw-exceptions false})

(def ^:dynamic *deadline* nil)

(defn request! [node request]
  (let [remaining (when *deadline* (quot (- *deadline* (System/nanoTime)) 1000000))]
    (when (and remaining (<= remaining 0))
      (throw (ex-info "recovery deadline exceeded" {:deadline true})))
    (let [timeouts (cond-> timeouts
                     remaining (assoc :socket-timeout (min 35000 remaining)
                                      :connection-timeout (min 1000 remaining)))
          request (cond-> request
                    (#{"audit" "await-reclaimed"} (:action request))
                    (assoc :budget_ms (max 1 (min 25000 (- (or remaining 35000) 1000)))))
          res (http/post (str "http://" (ip node) ":4441/operation")
                         (assoc timeouts :content-type :json :body (json/generate-string request)))
          body (try (json/parse-string (:body res) true) (catch Exception _ nil))]
      (if (= 200 (:status res))
        (if (map? body) body (throw (ex-info "invalid operation response" {:corrupt true})))
        (throw (ex-info "fork operation failed"
                        (cond-> {:status (:status res), :body body,
                                 :code (get-in body [:error :code])}
                          (= "audit_failed" (get-in body [:error :code]))
                          (assoc :corrupt true))))))))

(defn message-size [value]
  (alength (.getBytes (json/generate-string value) "UTF-8")))
(defn advance [offset value] (+ offset 4 (message-size value)))

(defn script [id paths now]
  (let [[a b c d e f] paths
        x (str id "-a") y (str id "-b") z (str id "-c") extra (str id "-extra")
        later (str id "-later") replacement (str id "-replacement")
        xy (advance (advance 0 x) y) xyz (advance xy z)
        far (+ now 3600000)]
    (mapv (fn [i op] (assoc op :id (str id "/" i))) (range)
          [{:action "create", :path a, :value x}
           {:action "append", :path a, :value y}
           {:action "fork", :source a, :path b, :at xy, :expires_at_ms far}
           {:action "append", :path a, :value extra}
           {:action "append", :path b, :value z}
           {:action "fork", :source b, :path c, :at xyz, :expires_at_ms far}
           {:action "copy", :source c, :path d}
           {:action "append", :path c, :value later}
           {:action "trim", :path b, :at xyz}
           {:action "delete", :path b}
           {:action "trim", :path a, :at (advance xy extra)}
           {:action "delete", :path a}
           {:action "read", :path c, :max_bytes 16, :retained true}
           {:action "read", :path d, :max_bytes 16}
           {:action "delete", :path c}
           {:action "await-reclaimed", :ancestors [a b c]}
           {:action "read", :path d, :max_bytes 16, :independent true}
           {:action "delete", :path d}
           {:action "create", :path a, :value replacement}
           {:action "fork", :source a, :path d, :at (advance 0 replacement), :expires_at_ms far}
           {:action "delete", :path a}
           {:action "read", :path d, :max_bytes 16, :retained true}
           {:action "create", :path e, :value (str id "-expiry"), :ttl_s 5}
           {:action "fork", :source e, :path f, :expires_at_ms far, :expiry-setup true}
           {:action "await-expiry", :path e}
           {:action "read", :path f, :max_bytes 16, :retained true}])))

(defn read-pages! [node request]
  (loop [at nil, pages []]
    (when (> (count pages) 10000) (throw (ex-info "page limit" {:corrupt true})))
    (let [page (try (request! node (cond-> request at (assoc :at at)))
                    (catch Exception e
                      (throw (ex-info "page read failed" (assoc (or (ex-data e) {}) :partial-pages pages) e))))
          pages (conj pages page)
          next (:next_offset page)]
      (cond
        (:up_to_date page) {:pages pages}
        (or (nil? next) (and at (<= next at)))
        (throw (ex-info "non-progressing cursor" {:corrupt true, :pages pages}))
        :else (recur next pages)))))

(defn execute! [node request]
  {:request request,
   :reply (if (= "read" (:action request)) (read-pages! node request) (request! node request))})

(defn scenario! [state node actor]
  (let [snapshot @state
        current (get-in snapshot [:scenarios (get-in snapshot [:actors actor])])]
    (if (and current (< (:stage current) (count (:script current))))
      current
      (let [id (str actor "-" (swap! (:next-id @state) inc))
            plan (request! node {:action "plan", :id id})
            scenario {:id id, :stage 0, :paths (:paths plan), :shards (:shards plan),
                      :script (script id (:paths plan) (:now_ms plan))}]
        (swap! state #(-> % (assoc-in [:actors actor] id) (assoc-in [:scenarios id] scenario)))
        scenario))))

(defn update-scenario! [state id f]
  (swap! state update-in [:scenarios id] f))

(defn restart-expiry! [state scenario]
  (let [attempt (inc (:expiry-attempt scenario 0))
        start (first (keep-indexed (fn [i request] (when (:ttl_s request) i)) (:script scenario)))
        old-e (get-in scenario [:script start :path])
        old-f (get-in scenario [:script (inc start) :path])
        e (str old-e "-retry" attempt) f (str old-f "-retry" attempt)
        rewrite (fn [request]
                  (cond-> (update request :id str "-retry" attempt)
                    (= old-e (:path request)) (assoc :path e)
                    (= old-f (:path request)) (assoc :path f)
                    (= old-e (:source request)) (assoc :source e)
                    (= "create" (:action request)) (update :value str "-retry" attempt)))
        updated (-> scenario (assoc :stage start :expiry-attempt attempt :attempted false)
                    (update :script #(into (subvec % 0 start) (map rewrite (subvec % start))))
                    (update :paths into [e f]))]
    (update-scenario! state (:id scenario) (constantly updated))))

(defn resolve-reclamation [scenario request]
  (if (and (= "await-reclaimed" (:action request)) (not (:sources request)))
    (let [created (into {} (for [{:keys [request reply]} (:completed scenario)
                                :when (#{"create" "fork" "copy"} (:action request))]
                            [(:path request) (assoc (select-keys reply [:sid :shard]) :path (:path request))]))
          sources (mapv created (:ancestors request))]
      (when-not (every? #(and (:sid %) (contains? % :shard)) sources)
        (throw (ex-info "missing ancestor identity" {:corrupt true})))
      (assoc request :sources sources))
    request))

(defn step! [state node scenario]
  (let [request (resolve-reclamation scenario (nth (:script scenario) (:stage scenario)))]
    (update-scenario! state (:id scenario)
                      #(-> % (assoc :attempted true) (assoc-in [:script (:stage scenario)] request)))
    (let [record (try (execute! node request)
                      (catch Exception e
                        (when (= 409 (:status (ex-data e)))
                          (update-scenario! state (:id scenario) #(assoc % :attempted false)))
                        (when (and (:expiry-setup request) (= 409 (:status (ex-data e)))
                                   (contains? #{"source_gone" "source_not_found"} (:code (ex-data e))))
                          (restart-expiry! state scenario))
                        (throw e)))]
      (update-scenario! state (:id scenario)
                        #(-> % (update :stage inc) (assoc :attempted false)
                             (update :completed (fnil conj []) record)))
      record)))

(defn missing? [data]
  (and (= 409 (:status data)) (contains? #{"gone" "not_found"} (:code data))))

(defn observe! [node request attempts expected]
  (loop []
    (let [result (try {:record (execute! node request)} (catch Exception e {:error e}))]
      (if-let [record (:record result)]
        record
        (let [error (:error result) data (ex-data error)
              partial (when (seq (:partial-pages data)) {:pages (:partial-pages data), :partial true})]
          (swap! attempts conj {:outcome :info, :request request, :reply partial})
          (cond
            (and (missing? data) (:deadline_ms expected))
            (let [expiry-request {:action "await-expiry", :path (:path request),
                                  :id (str (:id request) "/expiry"), :final true}
                  expiry (execute! node expiry-request)]
              (if (>= (get-in expiry [:reply :observed_at_ms]) (:deadline_ms expected))
                expiry
                (throw (ex-info "stream disappeared before expiry" {:corrupt true :request request}))))
            (or (:corrupt data) (= 409 (:status data)))
            (throw (ex-info "invalid recovery observation" (assoc data :corrupt true :request request) error))
            (>= (System/nanoTime) *deadline*) (throw error)
            :else (recur)))))))

(defn live-paths [scenario]
  (reduce (fn [paths {:keys [request reply]}]
            (case (:action request)
              ("create" "fork" "copy") (assoc paths (:path request) reply)
              ("delete" "await-expiry") (dissoc paths (:path request))
              paths)) {} (:completed scenario)))

(defn recover-before! [state node deadline]
  (let [steps (atom [])]
    (db/await-ready node)
    (doseq [id (keys (:scenarios @state))]
      (loop []
        (let [s (get-in @state [:scenarios id])
              request (get-in s [:script (:stage s)])]
          (when (and (:attempted s) (not= "read" (:action request)))
            (when (> (System/nanoTime) deadline) (throw (ex-info "recovery deadline" {:scenario id})))
            (let [result (try {:record (step! state node s)} (catch Exception e {:error e}))]
              (if-let [record (:record result)]
                (swap! steps conj record)
                (when (:corrupt (ex-data (:error result))) (throw (:error result)))))
            (recur)))))
    (let [scenarios (vals (:scenarios @state))
          final-reads (vec (for [s scenarios [path expected] (live-paths s)]
                              (observe! node {:action "read", :path path,
                                              :id (str (:id s) "/final/" path), :max_bytes 16,
                                              :final true} steps expected)))
          audit (:reply (observe! node {:action "audit", :paths (vec (mapcat :paths scenarios))} steps nil))]
      {:steps @steps, :final-reads final-reads, :audit audit,
       :plans (mapv #(select-keys % [:paths :shards]) scenarios)})))

(defn recover! [state node]
  (let [deadline (+ (System/nanoTime) 180000000000)]
    (binding [*deadline* deadline]
      (recover-before! state node deadline))))

(defrecord Client [node state]
  client/Client
  (open! [this _test node] (assoc this :node node))
  (setup! [_ _test])
  (invoke! [_ test op]
    (let [scenario (atom nil)]
      (try
        (case (:f op)
          :step (let [s (scenario! state node (mod (:process op) (:concurrency test 5)))]
                  (reset! scenario s)
                  (assoc op :type :ok, :value (step! state node s)))
          :recover (assoc op :type :ok, :value (recover! state node)))
        (catch Exception e
          (assoc op :type (if (or (:corrupt (ex-data e)) (= 409 (:status (ex-data e)))) :fail :info)
                    :value (when @scenario
                             (cond-> {:request (nth (:script @scenario) (:stage @scenario))}
                               (seq (:partial-pages (ex-data e)))
                               (assoc :reply {:pages (:partial-pages (ex-data e)), :partial true})))
                    :error (or (ex-data e) (.getMessage e)))))))
  (teardown! [_ _test])
  (close! [_ _test]))

(defn take-page [stream from max-bytes]
  (let [from (or from (:trim stream))
        available (filterv #(>= (first %) from) (:messages stream))
        messages (loop [left available, acc [], bytes 0]
                   (if (or (empty? left) (>= bytes max-bytes)) acc
                     (recur (subvec left 1) (conj acc (first left))
                            (+ bytes (message-size (second (first left)))))))
        next (if (= (count messages) (count available)) (:tail stream)
               (let [[at value] (last messages)
                     [next-at _] (nth available (count messages))]
                 (min (advance at value) next-at)))]
    (when (<= (:trim stream) from (:tail stream))
      {:messages messages, :next_offset next, :up_to_date (= next (:tail stream)),
       :closed (and (:closed stream) (= next (:tail stream)))})))

(defn valid-pages? [streams request reply]
  (let [pages (:pages reply)
        page (first pages)]
    (and page
         (some #(= page (take-page % (:at request) (:max_bytes request 16))) streams)
         (cond
           (:up_to_date page) (empty? (next pages))
           (next pages) (recur streams
                               (assoc request :at (:next_offset page))
                               (assoc reply :pages (vec (next pages))))
           :else (:partial reply)))))

(defn response-tail? [reply tail]
  (or (not (contains? reply :next_offset)) (= tail (:next_offset reply))))

(defn confirm-stream [stream reply]
  (when stream
    (let [confirmed (cond-> {}
                      (:sid reply) (assoc :sid (:sid reply))
                      (contains? reply :deadline_ms) (assoc :deadline (:deadline_ms reply)))]
      (when (every? (fn [[k v]] (or (not (contains? stream k))
                                   (and (= k :sid) (nil? (:sid stream)))
                                   (= v (get stream k)))) confirmed)
        (merge stream confirmed)))))

(defn apply-record [state {:keys [request reply recovery]}]
  (let [{:keys [action path source id value at]} request
        stream (get-in state [:streams path])
        src (get-in state [:streams source])
        old (get-in state [:applied id])
        capture (when (and reply (#{"fork" "copy"} action))
                  [(:captured_offset reply) (:inherited_sid reply) (:sid reply)])]
    (cond
      old (when (and (= request (:request old))
                     (response-tail? reply (:tail stream))
                     (or (nil? capture)
                         (every? true? (map (fn [a b] (or (nil? a) (nil? b) (= a b))) (:capture old) capture))))
            (when-let [confirmed (confirm-stream stream reply)]
              (cond-> (assoc-in state [:streams path] confirmed)
                capture (assoc-in [:applied id :capture]
                                  (mapv #(or %2 %1) (:capture old) capture)))))
      (= action "read")
      (let [versions (if (and (:stale-reads? state) (not recovery))
                       (get-in state [:versions path]) (when stream [stream]))]
        (when (valid-pages? versions request reply) state))
      (= action "await-reclaimed") (when (= (:sources request) (:reclaimed reply)) state)
      (= action "delete") (update state :streams dissoc path)
      (= action "await-expiry")
      (when (and stream reply (:expired reply) (:deadline stream)
                 (>= (:observed_at_ms reply) (:deadline stream)))
        (update state :streams dissoc path))
      (= action "trim") (when (and stream (<= at (:tail stream)))
                           (assoc-in state [:streams path :trim] (max at (:trim stream))))
      (= action "append") (when (and stream (response-tail? reply (advance (:tail stream) value)))
                             (-> state
                                 (update-in [:streams path :messages] conj [(:tail stream) value])
                                 (assoc-in [:streams path :tail] (advance (:tail stream) value))
                                 (assoc-in [:applied id] {:request request})))
      (= action "create") (when (and (nil? stream) (response-tail? reply (advance 0 value)))
                             (-> state
                                 (assoc-in [:streams path] (confirm-stream {:messages [[0 value]], :tail (advance 0 value), :trim 0, :closed false} reply))
                                 (assoc-in [:applied id] {:request request})))
      (#{"fork" "copy"} action)
      (when (and (nil? stream) src)
        (let [f (or at (:tail src)) p (min f (:trim src))
              messages (filterv (fn [[o _]] (<= p o (dec f))) (:messages src))]
          (when (and (<= f (:tail src)) (response-tail? reply f)
                     (or (nil? (:sid src)) (nil? reply) (= (:sid src) (:inherited_sid reply)))
                     (or (nil? reply) (= f (:captured_offset reply))))
            (-> state
                (assoc-in [:streams path] (confirm-stream {:messages messages, :tail f, :trim p, :closed (and (= "copy" action) (:closed src))} reply))
                (assoc-in [:applied id] {:request request, :capture (or capture [f (:sid src) nil])})))))
      :else nil)))

(defn transition [state record]
  (when-let [next (apply-record state record)]
    (let [path (get-in record [:request :path])
          before (get-in state [:streams path])
          after (get-in next [:streams path])]
      (if (and after (not= before after))
        (update-in next [:versions path] (fnil conj []) after) next))))

(defn records [history]
  (mapcat (fn [op]
            (cond
              (= :step (:f op)) (when (get-in op [:value :request]) [[(:type op) (:value op)]])
              (and (= :recover (:f op)) (= :ok (:type op)))
              (map #(vector (:outcome % :ok) (assoc % :recovery true)) (concat (get-in op [:value :steps]) (get-in op [:value :final-reads])))
              :else []))
          (remove #(= :invoke (:type %)) history)))

(defn history-errors
  ([history] (history-errors history {}))
  ([history opts]
  (let [groups (group-by (fn [[_ {:keys [request]}]]
                           (let [path (:path request)]
                             (if path (nth (str/split path #"/") 2 :unit) :unit))) (records history))
        invalid (keep (fn [[scenario records]]
                        (let [states (reduce (fn [states [type record]]
                                               (case type
                                                 :ok (set (keep #(transition % record) states))
                                                 :info (if (and (= "read" (get-in record [:request :action]))
                                                                (seq (get-in record [:reply :pages])))
                                                         (set (keep #(transition % record) states))
                                                         (into states (keep #(transition % record) states)))
                                                 :fail (if (seq (get-in record [:reply :pages]))
                                                         (set (keep #(transition % record) states)) states)
                                                 states))
                                             #{{:streams {}, :applied {}, :stale-reads? (= :distributed (:placement opts))}} records)]
                          (when (empty? states) scenario))) groups)]
    (cond-> []
      (seq invalid) (conj {:illegal-stream-histories (vec invalid)})
      (some #(get-in % [:error :corrupt]) history) (conj :invalid-observation)))))

(defn stream-identity [reply]
  (when (and (:sid reply) (contains? reply :shard)) (select-keys reply [:shard :sid])))

(defn observation-coverage [records]
  (select-keys
    (reduce
      (fn [state {:keys [request reply]}]
        (let [{:keys [action path source]} request
              stream (get-in state [:streams path])
              src (get-in state [:streams source])]
          (case action
            ("create" "fork" "copy")
            (let [confirmed (stream-identity reply)
                  identity (or confirmed (select-keys reply [:sid]))
                  ancestors (when (#{"fork" "copy"} action)
                              (conj (:dependencies src #{}) (:identity src)))
                  observed (if (and confirmed (= identity (:identity stream)))
                             stream
                             {:identity identity :copy? (= "copy" action)
                              :ancestors ancestors
                              :dependencies (if (= "fork" action) ancestors #{})})]
              (cond-> (assoc-in state [:streams path]
                               (assoc observed :source (when (= "fork" action)
                                                         [source (:inherited_sid reply)])))
                confirmed (update :known conj confirmed)))
            "delete" (update state :streams dissoc path)
            "await-expiry"
            (cond-> (update state :streams dissoc path)
              (get-in stream [:identity :sid])
              (update :expired conj [path (get-in stream [:identity :sid])]))
            "await-reclaimed"
            (let [sources (:sources request)
                  identities (map stream-identity sources)]
              (if (and (seq sources) (= sources (:reclaimed reply))
                       (every? #(and % (contains? (:known state) %)) identities))
                (update state :reclaimed into identities) state))
            "read"
            (if (and (seq (:pages reply)) (not (:partial reply)))
              (cond-> state
                (contains? (:expired state) (:source stream))
                (update :expired-source-reads inc)
                (and (:independent request) (:copy? stream)
                     (contains? (:known state) (:identity stream)) (seq (:ancestors stream))
                     (every? #(and % (contains? (:reclaimed state) %)) (:ancestors stream)))
                (update :independent-copy-reads inc))
              state)
            state)))
      {:streams {} :known #{} :expired #{} :reclaimed #{}
       :expired-source-reads 0 :independent-copy-reads 0} records)
    [:expired-source-reads :independent-copy-reads]))

(defn fork-checker []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [errors (history-errors history _test)
            recovery (last (filter #(and (= :recover (:f %)) (= :ok (:type %))) history))
            records (map second (filter #(= :ok (first %)) (records history)))
            coverage (observation-coverage records)
            actions (frequencies (map #(get-in % [:request :action]) records))
            missing (cond-> []
                      (nil? recovery) (conj :successful-recovery)
                      (not-any? #(= "read" (get-in % [:request :action]))
                                (get-in recovery [:value :final-reads])) (conj :final-reads)
                      (not= 0 (get-in recovery [:value :audit :rows])) (conj :cleanup-audit)
                      (not (pos? (get actions "fork" 0))) (conj :fork)
                      (not (pos? (get actions "copy" 0))) (conj :copy)
                      (not-any? #(get-in % [:request :retained]) records) (conj :retained-read)
                      (zero? (:independent-copy-reads coverage)) (conj :independent-copy-read)
                      (zero? (:expired-source-reads coverage)) (conj :expired-source-read)
                      (not-any? (fn [{:keys [shards]}] (and (seq shards) (every? (fn [[a b]] (not= a b)) (partition 2 1 shards))))
                                (get-in recovery [:value :plans])) (conj :cross-shard))]
        {:valid? (cond (seq errors) false (seq missing) :unknown :else true)
         :errors errors, :missing missing, :actions actions, :coverage coverage}))))

(defn workload [_opts]
  (let [state (atom {:next-id (atom 0), :actors {}, :scenarios {}})]
    {:client (->Client nil state)
     :generator (gen/repeat {:f :step})
     :final-generator (gen/once {:f :recover})
     :checker (fork-checker)}))
