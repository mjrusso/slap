(ns jepsen.slap.bank-test
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.test :refer [deftest is]]
            [jepsen.checker :as checker]
            [jepsen.history :as h]
            [jepsen.slap.bank :as bank]))

(def request {:id "1", :bank 0, :source 0, :destination 1, :amount 7,
              :body {:ops [{:op "put", :key "a", :value "new", :if_version 1}]}})
(defn transfer [type] {:type type, :f :transfer, :process 0, :value request})
(defn audit
  ([] (audit {0 993, 1 1007, 2 1000, 3 1000} [(bank/request-receipt request)]))
  ([balances receipts]
   {:type :ok, :f :audit, :process 0,
    :value {:bank 0, :cursor nil,
            :rows (vec (concat (for [[id balance] balances] [(bank/account-key id) (str balance)])
                               (for [receipt receipts] [(str "receipt/" (:id receipt)) (json/generate-string receipt)])))}}))
(defn serial-history [& ops]
  (h/history (mapcat (fn [op] [(assoc op :type :invoke) op]) ops)))
(defn errors [history placement] (bank/completed-errors {:placement placement} (bank/completions history)))

(deftest atomic-transfers-and-unknown-outcomes
  (doseq [placement [:object-lease :distributed]]
    (is (empty? (errors (serial-history (transfer :ok) (audit)) placement)))
    (is (empty? (errors (serial-history (transfer :info) (audit)) placement)))
    (is (empty? (errors (serial-history (transfer :info) (transfer :fail) (audit)) placement)))
    (is (empty? (errors (serial-history (transfer :info) (audit {0 1000, 1 1000, 2 1000, 3 1000} [])) placement)))
    (is (seq (errors (serial-history (transfer :fail) (audit)) placement)))))

(deftest partial-and-corrupt-application
  (doseq [bad [(audit {0 1000, 1 1000, 2 1000, 3 1000} [(bank/request-receipt request)])
               (audit {0 993, 1 1007, 2 1000, 3 1000} [])
               (audit {0 993, 1 1000, 2 1000, 3 1000} [(bank/request-receipt request)])
               (audit {0 993, 1 1007, 2 1000, 3 1000} [(assoc (bank/request-receipt request) :amount 8)])
               (update-in (audit) [:value :rows] conj ["foreign" "value"])
               (assoc-in (audit) [:value :cursor] "more")]]
    (is (seq (errors (serial-history (transfer :ok) bad) :object-lease)))))

(deftest freshness-depends-on-placement-and-recovery
  (let [empty-audit (audit {0 1000, 1 1000, 2 1000, 3 1000} [])]
    (is (seq (errors (serial-history (transfer :ok) empty-audit) :object-lease)))
    (is (empty? (errors (serial-history (transfer :ok) empty-audit) :distributed)))
    (is (seq (errors (serial-history (transfer :ok) (assoc empty-audit :recovery true)) :distributed)))
    (is (empty? (errors (h/history [(assoc empty-audit :type :invoke, :process 1)
                                   (transfer :invoke) (transfer :ok)
                                   (assoc empty-audit :process 1)]) :object-lease)))))

(deftest retry-identity-and-observed-overload
  (is (seq (errors (serial-history (transfer :info) (assoc-in (transfer :ok) [:value :body :ops 0 :if_version] 2) (audit)) :object-lease)))
  (is (seq (errors (serial-history (assoc (transfer :fail) :error {:type :overloaded}) (audit)) :object-lease)))
  (is (empty? (errors (serial-history (transfer :info) (assoc (transfer :fail) :error {:type :overloaded}) (audit)) :object-lease)))
  (doseq [[body kind] [[bank/overload-body :overloaded] ["service unavailable, retry" :unavailable]]]
    (with-redefs [http/post (fn [& _] {:status 503, :body body})]
      (is (= kind (try (bank/batch! "n1" 0 {})
                       (catch Exception e (:type (ex-data e)))))))))

(deftest receipts-cannot-be-justified-by-future-retries
  (doseq [placement [:object-lease :distributed]]
    (let [rejected (assoc (transfer :fail) :error {:type :overloaded})
          retry-invoke (assoc (transfer :invoke) :process 1)
          retry-unknown (assoc (transfer :info) :process 1)
          observed (assoc (audit) :process 2)
          audit-invoke (assoc observed :type :invoke)]
      (is (some #(= :receipt (:type %))
                (errors (h/history [(transfer :invoke) rejected audit-invoke observed
                                    retry-invoke retry-unknown]) placement)))
      (is (empty? (errors (h/history [(transfer :invoke) rejected audit-invoke retry-invoke
                                     observed retry-unknown]) placement))))))

(deftest pairing-preserves-history-indexes-and-rejects-unpaired-completions
  (let [history (h/history [(assoc (transfer :invoke) :index 10)
                           (assoc (transfer :ok) :index 20)
                           (assoc (audit) :type :invoke, :index 30)
                           (assoc (audit) :index 40)])]
    (is (= [[20 10] [40 30]]
           (mapv (juxt :index :invoke-index) (bank/completions history)))))
  (is (= [{:type :unpaired-completion, :index 1}]
         (errors (h/remove h/invoke? (serial-history (transfer :ok))) :object-lease)))
  (is (empty? (bank/completions (h/history [(transfer :invoke)])))))

(deftest recovery-and-coverage-are-required
  (is (= :unknown (:valid? (checker/check (bank/bank-checker) {} (h/history []) {}))))
  (let [ops (concat [{:type :ok, :f :ready, :process 0} (assoc (transfer :ok) :recovery true)]
                   (for [b (range bank/bank-count)]
                     (-> (if (zero? b) (audit) (audit {0 1000, 1 1000, 2 1000, 3 1000} []))
                         (assoc :recovery true) (assoc-in [:value :bank] b))))
        history (apply serial-history ops)]
    (is (= true (:valid? (checker/check (bank/bank-checker) {:placement :object-lease, :bank-admission :normal} history {}))))
    (is (= :unknown (:valid? (checker/check (bank/bank-checker) {:placement :object-lease} history {}))))
    (is (= true (:valid? (checker/check (bank/bank-checker) {:placement :object-lease}
                                       (apply serial-history (concat ops [{:f :admission, :type :ok, :process 0, :value {:overloaded 1}}])) {}))))))

(deftest client-retries-preserve-the-captured-request
  (let [state (atom {:next -1, :pending {}})
        sent (atom [])
        node-client (bank/->Client "n1" state)
        op {:type :invoke, :f :transfer, :process 0}]
    (with-redefs [bank/fetch! (fn [& _] {:balance 1000, :version 10})
                  bank/batch! (fn [_ _ body]
                                (swap! sent conj body)
                                (if (= 1 (count @sent))
                                  (throw (ex-info "unknown" {:type :unavailable}))
                                  {:conflict true}))]
      (let [a (jepsen.client/invoke! node-client {} op)
            b (jepsen.client/invoke! (bank/->Client "n2" state) {:concurrency 5} (assoc op :process 5))]
        (is (= :info (:type a)))
        (is (= :fail (:type b)))
        (is (= (:value a) (:value b)))
        (is (= (first @sent) (second @sent)))
        (is (empty? (:pending @state)))))))

(deftest malformed-pages-fail-instead-of-looking-unavailable
  (doseq [page [{:rows 1, :cursor nil} {:rows []}
                {:rows [{:key "!", :value "MQ"}], :cursor nil}
                {:rows [{:key nil, :value "MQ"}], :cursor nil}]]
    (with-redefs [http/get (fn [& _] {:status 200, :body (json/generate-string page)})]
      (let [op (jepsen.client/invoke! (bank/->Client "n1" (atom {})) {}
                                     {:type :invoke, :process 0, :f :audit, :value 0})]
        (is (= :corrupt (get-in op [:error :type])))
        (is (seq (errors (serial-history op) :object-lease)))))))

(deftest exhausted-ordinary-transfers-leave-room-for-recovery
  (let [workload (bank/workload {})
        node-client (:client workload)
        state (:state node-client)
        ordinary-limit (* bank/bank-count (dec bank/max-receipts))
        ordinary (repeatedly ordinary-limit #(bank/next-request! state))]
    (is (= ordinary-limit (count (set (map :id (doall ordinary))))))
    (is (nil? (bank/next-request! state)))
    (is (every? #(not= :transfer (:f %)) (repeatedly 20 (:generator workload))))
    (with-redefs [bank/fetch! (fn [& _] {:balance 1000, :version 10})
                  bank/batch! (fn [& _] {:json {:version 11}})]
      (let [recovered (jepsen.client/invoke! node-client {}
                        {:type :invoke, :f :transfer, :process 0, :recovery true})]
        (is (= :ok (:type recovered)))
        (is (= (str ordinary-limit) (get-in recovered [:value :id]))))
      (let [remaining (doall (repeatedly (dec bank/bank-count) #(bank/next-request! state true)))]
        (is (= (set (range 1 bank/bank-count)) (set (map :bank remaining))))
        (is (nil? (bank/next-request! state true)))))))
