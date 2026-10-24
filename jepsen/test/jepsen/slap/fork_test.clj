(ns jepsen.slap.fork-test
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [jepsen.slap.docker :as docker]
            [clojure.test :refer [deftest is]]
            [jepsen.checker :as checker]
            [jepsen.slap.fork :as fork]
            [jepsen.slap.db :as db]))

(defn event [type request reply]
  {:type type, :f :step, :value {:request request, :reply reply}})

(def create {:action "create", :path "a", :id "create-a", :value 1})
(def append {:action "append", :path "a", :id "append-a", :value 2})
(def reference {:action "fork", :source "a", :path "b", :id "fork-b", :at 10})
(def read-b {:action "read", :path "b", :id "read-b", :max_bytes 1})
(def prefix {:captured_offset 10, :inherited_sid 1, :sid 2})
(def good-read {:pages [{:messages [[0 1]], :next_offset 5, :up_to_date false, :closed false}
                       {:messages [[5 2]], :next_offset 10, :up_to_date true, :closed false}]})

(def base [(event :ok create {}) (event :ok append {}) (event :ok reference prefix)])

(deftest inherited-pages-must-be-complete-and-have-valid-cursors
  (is (empty? (fork/history-errors (conj base (event :ok read-b good-read)))))
  (doseq [bad [(assoc-in good-read [:pages 0 :next_offset] 6)
               (assoc-in good-read [:pages 1 :messages] [])
               (assoc-in good-read [:pages 1 :messages] [[5 3]])
               (assoc-in good-read [:pages 1 :messages] [[5 2] [10 3]])
               (update good-read :pages pop)]]
    (is (seq (fork/history-errors (conj base (event :ok read-b bad)))))))

(deftest unknown-appends-may-commit-or-not
  (let [read-a {:action "read", :path "a", :id "read-a", :max_bytes 10}
        page (fn [values tail] {:pages [{:messages values, :next_offset tail,
                                        :up_to_date true, :closed false}]})]
    (doseq [[values tail] [[[[0 1]] 5] [[[0 1] [5 2]] 10]]]
      (is (empty? (fork/history-errors [(event :ok create {})
                                       (event :info append nil)
                                       (event :ok read-a (page values tail))]))))))

(deftest retry-identity-keeps-one-captured-prefix
  (let [unknown (event :info reference nil)
        retry (event :ok reference prefix)]
    (is (empty? (fork/history-errors (concat (take 2 base) [unknown retry (event :ok read-b good-read)]))))
    (is (seq (fork/history-errors (concat base [(event :ok reference (assoc prefix :captured_offset 5))]))))
    (is (seq (fork/history-errors (concat base [(event :ok (assoc reference :source "other") prefix)]))))))

(deftest successful-retries-confirm-unknown-stream-identities
  (let [history [(event :ok create {:sid 1}) (event :ok append {})
                 (event :info reference nil)
                 (event :ok {:action "delete", :path "a", :id "delete-a"} {})
                 (event :ok reference prefix)]
        child {:action "fork", :source "b", :path "c", :id "fork-c", :at 10}]
    (is (empty? (fork/history-errors (conj history (event :ok child (assoc prefix :sid 3 :inherited_sid 2))))))
    (is (seq (fork/history-errors (conj history (event :ok child (assoc prefix :sid 3 :inherited_sid 99))))))))

(deftest successful-retries-confirm-deadlines-and-reject-conflicting-confirmations
  (let [history [(event :info create nil) (event :ok append {})
                 (event :ok create {:sid 1 :deadline_ms 100})]
        expiry {:action "await-expiry", :path "a", :id "expire-a"}]
    (is (empty? (fork/history-errors (conj history (event :ok expiry {:expired true :observed_at_ms 100})))))
    (doseq [reply [{:sid 2 :deadline_ms 100} {:sid 1 :deadline_ms 101} {:sid 1 :deadline_ms nil}]]
      (is (seq (fork/history-errors (conj history (event :ok create reply))))))))

(deftest definite-failures-do-not-append
  (let [read-a {:action "read", :path "a", :id "read-a", :max_bytes 10}]
    (is (seq (fork/history-errors [(event :ok create {}) (event :fail append nil)
                                  (event :ok read-a {:pages [{:messages [[0 1] [5 2]], :next_offset 10,
                                                             :up_to_date true, :closed false}]})])))))

(deftest retained-forks-and-copies-keep-captured-data-after-ancestor-removal
  (let [copy {:action "copy", :source "b", :path "c", :id "copy-c"}
        history (concat base [(event :ok copy (assoc prefix :inherited_sid 2 :sid 3))
                              (event :ok {:action "delete", :path "a", :id "delete-a"} {})
                              (event :ok {:action "delete", :path "b", :id "delete-b"} {})
                              (event :ok (assoc read-b :path "c") good-read)])]
    (is (empty? (fork/history-errors history)))))

(deftest unavailable-recovery-is-not-a-pass
  (is (= :unknown (:valid? (checker/check (fork/fork-checker) {} base {})))))

(defn coverage-count [key records]
  (key (fork/observation-coverage records)))

(deftest recovery-expiry-coverage-requires-the-captured-source-incarnation
  (let [created {:request create :reply {:sid 1}}
        forked {:request reference :reply prefix}
        expired {:request {:action "await-expiry" :path "a"} :reply {:expired true}}
        read {:request (assoc read-b :final true) :reply good-read}
        replacement {:request create :reply {:sid 3}}]
    (is (= 0 (coverage-count :expired-source-reads [created forked read])))
    (is (= 1 (coverage-count :expired-source-reads [created forked expired read])))
    (is (= 0 (coverage-count :expired-source-reads [created forked replacement expired read])))
    (is (= 0 (coverage-count :expired-source-reads [created forked expired
                                      {:request (assoc reference :action "copy") :reply prefix} read])))))

(deftest final-recovery-reads-count-after-acknowledged-source-expiry
  (let [history [(event :ok create {:sid 1 :deadline_ms 1})
                 (event :ok append {})
                 (event :ok reference prefix)
                 (event :ok {:action "await-expiry" :path "a" :id "expire-a"}
                        {:expired true :observed_at_ms 1})
                 {:type :ok :f :recover
                  :value {:final-reads [{:request (assoc read-b :final true) :reply good-read}]
                          :audit {:rows 0} :plans [{:shards [0 1]}]}}]
        result (checker/check (fork/fork-checker) {} history {})]
    (is (empty? (:errors result)))
    (is (not-any? #{:expired-source-read} (:missing result)))))

(deftest placement-specific-stale-reads-and-recovery
  (let [append-later {:action "append", :path "b", :id "append-b", :value 3}
        read-record {:request read-b, :reply good-read}
        history (conj base (event :ok append-later {}) (event :ok read-b good-read))]
    (is (seq (fork/history-errors history {:placement :object-lease})))
    (is (empty? (fork/history-errors history {:placement :distributed})))
    (is (seq (fork/history-errors (conj (vec (butlast history))
                                       {:type :ok, :f :recover,
                                        :value {:final-reads [read-record]}})
                                  {:placement :distributed})))))

(deftest successful-pages-before-an-unavailable-page-are-still-checked
  (let [partial (assoc good-read :pages [(first (:pages good-read))] :partial true)]
    (is (empty? (fork/history-errors (conj base (event :info read-b partial)))))
    (is (seq (fork/history-errors (conj base (event :info read-b (assoc-in partial [:pages 0 :messages] [[0 99]]))))))))

(deftest nonboundary-pagination-cannot-skip-target-owned-data
  (let [reference (assoc reference :at 3)
        append-b {:action "append", :path "b", :id "append-b", :value 3}
        reply {:pages [{:messages [[0 1]], :next_offset 3, :up_to_date false, :closed false}
                       {:messages [[3 3]], :next_offset 8, :up_to_date true, :closed false}]}
        history [(event :ok create {}) (event :ok reference (assoc prefix :captured_offset 3))
                 (event :ok append-b {}) (event :ok read-b reply)]]
    (is (empty? (fork/history-errors history)))))

(deftest recovery-resolves-attempted-creation-and-stops-planned-lifecycle-mutations
  (let [create-record {:request create :reply {:next_offset 5}}
        fork-request (assoc reference :at 5)
        scenario {:id "s", :stage 1, :attempted true, :paths ["a" "b"], :shards [0 1],
                  :completed [create-record],
                  :script [create fork-request {:action "delete", :path "a", :id "delete-a"}]}
        state (atom {:scenarios {"s" scenario}, :actors {0 "s"}})
        actions (atom [])
        page {:pages [{:messages [[0 1]], :next_offset 5, :up_to_date true, :closed false}]}]
    (with-redefs [db/await-ready (fn [_] nil)
                  fork/execute! (fn [_ request]
                                  (swap! actions conj (:action request))
                                  {:request request,
                                   :reply (case (:action request)
                                            "fork" {:next_offset 5 :captured_offset 5}
                                            "read" page
                                            "audit" {:rows 0})})]
      (let [result (fork/recover! state "n1")]
        (is (= ["fork" "read" "read" "audit"] @actions))
        (is (= 2 (count (:final-reads result))))))))

(deftest actors-resolve-current-scenarios-through-their-identifiers
  (let [state (atom {:next-id (atom 0), :actors {}, :scenarios {}})
        plans (atom 0)]
    (with-redefs [fork/request! (fn [_ _]
                                 (swap! plans inc)
                                 {:paths ["a" "b" "c" "d" "e" "f"] :shards [0 1 0 1 0 1] :now_ms 0})]
      (let [first (fork/scenario! state "n1" 0)
            id (:id first)]
        (fork/update-scenario! state id #(assoc % :stage 2 :attempted true))
        (let [retried (fork/scenario! state "n1" 0)]
          (is (= id (:id retried)))
          (is (= 2 (:stage retried)))
          (is (:attempted retried)))
        (is (= 1 @plans))
        (fork/update-scenario! state id #(assoc % :stage (count (:script %))))
        (let [next (fork/scenario! state "n1" 0)]
          (is (not= id (:id next)))
          (is (= 0 (:stage next)))
          (is (= 2 @plans))
          (is (= (:id next) (get-in @state [:actors 0])))
          (is (contains? (:scenarios @state) id)))))))

(deftest http-errors-are-decoded-into-structured-codes
  (doseq [[status body code] [[409 {:error {:code "not_found" :message "changed diagnostic"}} "not_found"]
                             [409 {:error {:code "conflict" :message ":not_found source_gone"}} "conflict"]
                             [503 {:error {:code "unavailable"}} "unavailable"]]]
    (with-redefs [docker/ip identity
                  http/post (fn [& _] {:status status :body (json/generate-string body)})]
      (let [data (try (fork/request! "node" {:action "read"})
                      (catch clojure.lang.ExceptionInfo e (ex-data e)))]
        (is (= {:status status :body body :code code} data))
        (is (= (= code "not_found") (boolean (fork/missing? data)))))))
  (with-redefs [docker/ip identity
                http/post (fn [& _] {:status 503 :body "proxy unavailable :not_found"})]
    (let [data (try (fork/request! "node" {})
                    (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (is (= 503 (:status data)))
      (is (nil? (:code data)))
      (is (not (fork/missing? data))))))

(deftest cleanup-diagnostics-are-fatal-observations
  (let [body {:error {:code "audit_failed" :message "ArgumentError with stack trace"}}]
    (with-redefs [docker/ip identity
                  http/post (fn [& _] {:status 500 :body (json/generate-string body)})]
      (let [data (try (fork/request! "node" {:action "audit" :paths []})
                      (catch clojure.lang.ExceptionInfo e (ex-data e)))
            history [{:type :fail :f :recover :error data}]
            result (checker/check (fork/fork-checker) {} history {})]
        (is (true? (:corrupt data)))
        (is (= body (:body data)))
        (is (false? (:valid? result)))
        (is (some #{:invalid-observation} (:errors result)))))))

(deftest expiry-restarts-use-error-codes-only
  (doseq [[code message restart?] [["source_gone" "changed diagnostic" true]
                                   ["source_not_found" "changed diagnostic" true]
                                   ["conflict" "source_gone :not_found" false]]]
    (let [script (fork/script "expiry" ["a" "b" "c" "d" "e" "f"] 0)
          stage (first (keep-indexed #(when (:expiry-setup %2) %1) script))
          scenario {:id "expiry" :script script :stage stage :paths ["e" "f"]}
          state (atom {:scenarios {"expiry" scenario}})]
      (with-redefs [fork/execute! (fn [& _] (throw (ex-info "failure" {:status 409 :code code :body message})))]
        (try (fork/step! state "node" scenario) (catch clojure.lang.ExceptionInfo _)))
      (is (= restart? (= 1 (get-in @state [:scenarios "expiry" :expiry-attempt])))))))

(deftest independent-copy-coverage-requires-physical-ancestor-reclamation
  (let [copy {:action "copy" :source "b" :path "c" :id "copy-c"}
        history [(event :ok create {:sid 1 :shard 0})
                 (event :ok append {})
                 (event :ok reference (assoc prefix :shard 1))
                 (event :ok copy {:captured_offset 10 :inherited_sid 2 :sid 3 :shard 0})
                 (event :ok {:action "delete" :path "a" :id "delete-a"} {})
                 (event :ok {:action "delete" :path "b" :id "delete-b"} {})
                 (event :ok (assoc read-b :path "c" :independent true) good-read)]
        result (checker/check (fork/fork-checker) {} history {})]
    (is (some #{:independent-copy-read} (:missing result)))))

(deftest independent-copy-reads-follow-identity-specific-reclamation-evidence
  (let [created {:request create :reply {:sid 1 :shard 0}}
        forked {:request reference :reply (assoc prefix :shard 1)}
        copied {:request {:action "copy" :source "b" :path "c"}
                :reply {:sid 3 :shard 2 :inherited_sid 2 :captured_offset 10}}
        sources [{:path "a" :sid 1 :shard 0} {:path "b" :sid 2 :shard 1}]
        reclaimed {:request {:action "await-reclaimed" :sources sources}
                   :reply {:reclaimed sources}}
        read {:request (assoc read-b :path "c" :independent true) :reply good-read}
        records [created forked copied reclaimed read]]
    (is (= 1 (coverage-count :independent-copy-reads records)))
    (is (= 0 (coverage-count :independent-copy-reads (update-in records [2 :reply] dissoc :shard))))
    (is (= 0 (coverage-count :independent-copy-reads [created forked copied read reclaimed])))
    (is (= 0 (coverage-count :independent-copy-reads (assoc-in records [3 :reply :reclaimed 0 :sid] 99))))
    (is (= 0 (coverage-count :independent-copy-reads
                             (-> records
                                 (assoc-in [3 :request :sources 0 :sid] 99)
                                 (assoc-in [3 :reply :reclaimed 0 :sid] 99)))))
    (is (= 0 (coverage-count :independent-copy-reads (assoc-in records [2 :request :action] "fork"))))
    (is (= 0 (coverage-count :independent-copy-reads (assoc-in records [4 :reply :partial] true))))
    (is (= 0 (coverage-count :independent-copy-reads
                             (into (pop records)
                                   [{:request {:action "create" :path "b"} :reply {:sid 4 :shard 1}}
                                    {:request {:action "copy" :source "b" :path "c"}
                                     :reply {:sid 5 :shard 2 :inherited_sid 4 :captured_offset 10}}
                                    read]))))))

(deftest cleanup-requests-use-the-remaining-http-budget
  (doseq [action ["audit" "await-reclaimed"]]
    (with-redefs [docker/ip identity
                  http/post (fn [_ options]
                              (let [body (json/parse-string (:body options) true)]
                                (is (pos? (:budget_ms body)))
                                (is (< (:budget_ms body) (:socket-timeout options)))
                                (is (<= (:socket-timeout options) 2000)))
                              {:status 200 :body "{}"})]
      (binding [fork/*deadline* (+ (System/nanoTime) 2000000000)]
        (fork/request! "node" {:action action})))))

(deftest observation-coverage-shares-provenance-across-expiry-and-copy-retries
  (let [sources [{:path "a" :sid 1 :shard 0} {:path "b" :sid 2 :shard 1}]
        copied {:request {:action "copy" :source "b" :path "c"}
                :reply {:sid 3 :shard 2 :inherited_sid 2 :captured_offset 10}}
        records [{:request create :reply {:sid 1 :shard 0}}
                 {:request reference :reply (assoc prefix :shard 1)}
                 copied
                 {:request {:action "await-expiry" :path "a"} :reply {:expired true}}
                 {:request read-b :reply good-read}
                 {:request {:action "delete" :path "b"} :reply {}}
                 {:request {:action "await-reclaimed" :sources sources}
                  :reply {:reclaimed sources}}
                 copied
                 {:request (assoc read-b :path "c" :independent true) :reply good-read}]]
    (is (= {:expired-source-reads 1 :independent-copy-reads 1}
           (fork/observation-coverage records)))))
