(ns jepsen.slap.bank
  "Conditional account transfers with immutable receipts, audited in one scan page."
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.set :as set]
            [clojure.string :as str]
            [jepsen [checker :as checker] [client :as client] [generator :as gen]]
            [jepsen.history :as h]
            [jepsen.slap.db :as db]
            [jepsen.slap.docker :refer [ip]])
  (:import (java.util Base64) (java.net URLEncoder)))

(def bank-count 64)
(def account-count 4)
(def initial-balance 1000)
(def max-receipts 500)
(def timeouts {:socket-timeout 10000, :connection-timeout 1000, :throw-exceptions false,
               :retry-handler (fn [_exception _count _context] false)})
(def overload-body "overloaded: request was not applied")

(defn encode64 [s] (.encodeToString (.withoutPadding (Base64/getUrlEncoder)) (.getBytes (str s) "UTF-8")))
(defn decode64 [s]
  (when-not (string? s) (throw (ex-info "non-string row encoding" {:type :corrupt, :value s})))
  (try (String. (.decode (Base64/getUrlDecoder) ^String s) "UTF-8")
       (catch IllegalArgumentException e
         (throw (ex-info "malformed row encoding" {:type :corrupt, :value s} e)))))
(defn url [node bank] (str "http://" (ip node) ":4437/v1/kv/bank-" bank))
(defn account-key [id] (str "account/" id))

(defn request! [method url options]
  (let [{:keys [status body] :as response} (method url (merge timeouts options))]
    (cond
      (= 200 status) (try (assoc response :json (json/parse-string body true))
                          (catch Exception e (throw (ex-info "malformed JSON response" {:type :corrupt, :body body} e))))
      (= 412 status) (assoc response :conflict true)
      (= 503 status) (throw (ex-info "batch unavailable" {:type (if (= overload-body body) :overloaded :unavailable)}))
      (<= 500 status) (throw (ex-info "server failure" {:type :unavailable, :status status, :body body}))
      :else (throw (ex-info "unexpected HTTP response" {:type :http, :status status, :body body})))))

(defn batch! [node bank body]
  (request! http/post (url node bank) {:content-type :json, :body (json/generate-string body)}))

(defn scan! [node bank]
  (let [result (:json (request! http/get (str (url node bank) "?limit=1000") {}))]
    (when-not (and (map? result) (vector? (:rows result)) (contains? result :cursor))
      (throw (ex-info "malformed scan page" {:type :corrupt, :page result})))
    {:bank bank, :cursor (:cursor result),
     :rows (mapv (fn [{:keys [key value]}] [(decode64 key) (decode64 value)]) (:rows result))}))

(defn fetch! [node bank account]
  (let [{:keys [status body headers]} (http/get (str (url node bank) "/" (URLEncoder/encode (account-key account) "UTF-8")) timeouts)]
    (if (= 200 status)
      {:balance (parse-long body), :version (some-> (get headers "etag") (str/replace "\"" "") parse-long)}
      (throw (ex-info "account read failed" {:type :prewrite, :status status})))))

(defn request-receipt [request] (select-keys request [:id :source :destination :amount]))

(defn capture! [node {:keys [bank source destination amount] :as request}]
  (let [a (fetch! node bank source)
        b (fetch! node bank destination)]
    (when-not (and (integer? (:balance a)) (integer? (:balance b))
                   (integer? (:version a)) (integer? (:version b)))
      (throw (ex-info "malformed account" {:type :corrupt})))
    (when (< (:balance a) amount)
      (throw (ex-info "insufficient balance" {:type :prewrite})))
    (assoc request :body
           {:ops [{:op "put", :key (encode64 (account-key source)),
                   :value (encode64 (- (:balance a) amount)), :if_version (:version a)}
                  {:op "put", :key (encode64 (account-key destination)),
                   :value (encode64 (+ (:balance b) amount)), :if_version (:version b)}
                  {:op "put", :key (encode64 (str "receipt/" (:id request))),
                   :value (encode64 (json/generate-string (request-receipt request))), :if_absent true}]})))

(defn initialize! [node bank]
  (let [body {:ops (mapv (fn [a] {:op "put", :key (encode64 (account-key a)),
                                 :value (encode64 initial-balance), :if_absent true})
                        (range account-count))}]
    (loop [attempts 100]
      (let [result (try (batch! node bank body) :ready
                        (catch Exception e (if (#{:overloaded :unavailable} (:type (ex-data e))) :retry (throw e))))]
        (when (= :retry result)
          (when (zero? attempts) (throw (ex-info "bank initialization unavailable" {:bank bank})))
          (Thread/sleep 20)
          (recur (dec attempts)))))))

(defn ordinary-limit [] (* bank-count (dec max-receipts)))

(defn next-request!
  ([state] (next-request! state false))
  ([state recovery?]
   (let [counter (if recovery? :next-recovery :next)
         start (if recovery? (dec (ordinary-limit)) -1)
         limit (if recovery? (* bank-count max-receipts) (ordinary-limit))
         id (get (swap! state update counter #(min limit (inc (or % start)))) counter)
         bank (mod id bank-count)
         source (mod (quot id bank-count) account-count)]
     (when (< id limit)
       {:id (str id), :bank bank, :source source,
        :destination (mod (inc source) account-count), :amount (inc (mod id 7))}))))

(defn transfer! [node state actor op]
  (let [request (or (get-in @state [:pending actor]) (next-request! state (:recovery op)))]
    (if-not request
      (assoc op :type :fail, :error :bank-capacity)
      (try
        (let [request (if (:body request) request (capture! node request))]
          (swap! state assoc-in [:pending actor] request)
          (let [response (batch! node (:bank request) (:body request))
                version (get-in response [:json :version])]
            (when (and (not (:conflict response)) (not (nat-int? version)))
              (throw (ex-info "invalid batch version" {:type :corrupt, :response response})))
            (swap! state update :pending dissoc actor)
            (assoc op :type (if (:conflict response) :fail :ok), :value request,
                   :version version)))
        (catch Exception e
          (let [request (get-in @state [:pending actor] request)
                kind (:type (ex-data e))
                submitted? (contains? request :body)]
            (when (#{:prewrite :corrupt :http} kind)
              (swap! state update :pending dissoc actor))
            (assoc op :type (if (and submitted? (not (#{:overloaded :http} kind))) :info :fail),
                   :value request, :error (or (ex-data e) {:message (.getMessage e)}))))))))

(defn admission! [node bank]
  (let [calls (doall (repeatedly 8 #(future
                                    (try (batch! node bank {:ops [{:op "check", :key (encode64 "probe"), :if_absent true}]})
                                         :ok
                                         (catch Exception e (or (:type (ex-data e)) :unavailable))))))]
    (frequencies (mapv deref calls))))

(defrecord Client [node state]
  client/Client
  (open! [this _test node] (assoc this :node node))
  (setup! [_ _test] (doseq [bank (range bank-count)] (initialize! node bank)))
  (invoke! [_ test op]
    (try
      (case (:f op)
        :ready (db/ready-op node op)
        :transfer (transfer! node state (mod (:process op) (:concurrency test 5)) op)
        :audit (assoc op :type :ok, :value (scan! node (:value op)))
        :admission (assoc op :type :ok, :value (admission! node (:value op))))
      (catch Exception e
        (assoc op :type :fail, :error (or (ex-data e) {:message (.getMessage e)})))))
  (teardown! [_ _test])
  (close! [_ _test]))

(defn completions [history]
  (mapv (fn [op] (assoc op :invoke-index (:index (h/invocation history op))))
        (h/remove h/invoke? (h/client-ops history))))

(defn request-errors [requests]
  (for [[id ops] requests
        :when (> (count (set (map :value ops))) 1)]
    {:type :retry-identity, :id id}))

(defn audit-errors [test requests acknowledged op]
  (let [{:keys [bank rows cursor]} (:value op)
        account-rows (filter #(str/starts-with? (first %) "account/") rows)
        receipt-rows (filter #(str/starts-with? (first %) "receipt/") rows)
        accounts (into {} (map (fn [[k v]] [(parse-long (subs k 8)) (parse-long v)]) account-rows))
        receipts (mapv (fn [[key value]]
                         (try {:id (subs key 8), :receipt (json/parse-string value true)}
                              (catch Exception _ {:id (subs key 8), :receipt :invalid}))) receipt-rows)
        visible (set (map :id receipts))
        expected (reduce (fn [balances {:keys [receipt]}]
                           (if (and (map? receipt) (integer? (:source receipt)) (integer? (:destination receipt))
                                    (integer? (:amount receipt)) (contains? balances (:source receipt))
                                    (contains? balances (:destination receipt)))
                             (-> balances (update (:source receipt) - (:amount receipt))
                                 (update (:destination receipt) + (:amount receipt)))
                             balances))
                         (zipmap (range account-count) (repeat initial-balance)) receipts)
        required (set (for [ack acknowledged
                            :when (and (= bank (get-in ack [:value :bank]))
                                       (or (:recovery op)
                                           (and (= :object-lease (:placement test))
                                                (< (:index ack) (:invoke-index op)))))]
                        (get-in ack [:value :id])))]
    (concat
      (when (some? cursor) [{:type :incomplete-page, :bank bank, :cursor cursor}])
      (when (or (not= account-count (count account-rows))
                (not= (set (range account-count)) (set (keys accounts)))
                (not-every? #(and (integer? %) (<= 0 %)) (vals accounts)))
        [{:type :accounts, :bank bank}])
      (when (or (not= (count rows) (+ (count account-rows) (count receipt-rows)))
                (not= (count rows) (count (set (map first rows)))))
        [{:type :row-keys, :bank bank}])
      (for [{:keys [id receipt]} receipts
            :let [attempts (filter #(< (:invoke-index %) (:index op)) (get requests id))
                  original (:value (first attempts))]
            :when (not (and (= bank (:bank original)) (= receipt (request-receipt original))
                            (some #(#{:ok :info} (:type %)) attempts)))]
        {:type :receipt, :bank bank, :id id})
      (when (not= expected accounts) [{:type :balances, :bank bank, :expected expected, :actual accounts}])
      (when-let [missing (seq (set/difference required visible))]
        [{:type :missing-acknowledged, :bank bank, :ids (set missing)}]))))

(defn completed-errors [test completed]
  (let [paired (filter :invoke-index completed)
        attempts (filter #(and (= :transfer (:f %)) (get-in % [:value :id])) paired)
        requests (group-by #(get-in % [:value :id]) attempts)
        acknowledged (filter #(= :ok (:type %)) attempts)
        audits (filter #(and (= :audit (:f %)) (= :ok (:type %))) paired)]
    (vec (concat (for [op completed :when (nil? (:invoke-index op))]
                   {:type :unpaired-completion, :index (:index op)})
                 (request-errors requests)
                 (for [op completed :when (or (= :corrupt (get-in op [:error :type]))
                                               (and (= :admission (:f op))
                                                    (some pos? (map #(get-in op [:value %] 0) [:http :corrupt]))))]
                   {:type :corrupt})
                 (mapcat #(audit-errors test requests acknowledged %) audits)))))

(defn bank-checker []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [completed (completions history)
            errors (completed-errors test completed)
            transfers (count (filter #(and (= :transfer (:f %)) (= :ok (:type %))) completed))
            audits (filter #(and (= :audit (:f %)) (= :ok (:type %))) completed)
            recovered (set (map #(get-in % [:value :bank]) (filter :recovery audits)))
            overloads (+ (count (filter #(= :overloaded (get-in % [:error :type])) completed))
                         (reduce + 0 (map #(get-in % [:value :overloaded] 0)
                                          (filter #(= :admission (:f %)) completed))))
            recovery-transfers (count (filter #(and (= :transfer (:f %)) (= :ok (:type %)) (:recovery %)) completed))
            ready? (some #(and (= :ready (:f %)) (= :ok (:type %))) completed)
            low? (not= :normal (:bank-admission test))]
        {:valid? (cond (seq errors) false
                       (and ready? (pos? transfers) (pos? recovery-transfers) (= (set (range bank-count)) recovered)
                            (or (not low?) (pos? overloads))) true
                       :else :unknown)
         :transfers transfers, :recovery-transfers recovery-transfers, :audits (count audits), :overloads overloads,
         :recovered recovered, :errors (take 10 errors)}))))

(defn workload [_opts]
  (let [state (atom {:next -1, :pending {}})
        next-bank (atom -1)]
    {:client (->Client nil state)
     :generator (fn []
                  (let [n (swap! next-bank inc) bank (mod n bank-count)]
                    (case (mod n 10)
                      0 {:f :admission, :value bank}
                      (1 2 3) {:f :audit, :value bank}
                      (if (< (:next @state) (dec (ordinary-limit)))
                        {:f :transfer}
                        {:f :audit, :value bank}))))
     :final-generator (gen/phases
                        (gen/time-limit 60 (gen/until-ok (gen/stagger 1 (repeat {:f :ready}))))
                        (gen/time-limit 30 (gen/until-ok (gen/stagger 0.01 (repeat {:f :transfer, :recovery true}))))
                        (map (fn [bank] {:f :audit, :value bank, :recovery true}) (range bank-count)))
     :checker (bank-checker)}))
