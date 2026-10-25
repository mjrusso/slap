(ns jepsen.slap.log-fork-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.checker :as checker]
            [clj-http.client :as http]
            [jepsen.slap.docker :as docker]
            [jepsen.client :as client]
            [jepsen.history :as h]
            [jepsen.slap.db :as db]
            [jepsen.slap.log-fork :as fork]))

(defn record [action key reply & args]
  {:request (merge {:action action :key key} (apply hash-map args)), :reply reply})

(def prefix
  [(record "initialize" "a" {:offset 0})
   (record "append" "a" {:offset 7} :value "x")
   (record "append" "a" {:offset 14} :value "y")
   (record "compact" "a" {:offset 7})
   (record "fork" "b" {:offset 14 :snapshot_offset 7} :source "a")
   (record "append" "b" {:offset 23} :value "own")
   (record "delete" "a" {:deleted true})
   (record "read" "b" {:offset 23 :values ["x" "y" "own"]})])

(deftest captured-prefix-and-target-suffix
  (doseq [placement [:object-lease :distributed]]
    (is (empty? (fork/validate-records placement prefix)))
    (doseq [values [["y" "own"] ["x" "y" "extra" "own"] ["y" "x" "own"] ["x" "y" "y" "own"]]]
      (is (seq (fork/validate-records placement (assoc-in prefix [7 :reply :values] values)))))
    (is (seq (fork/validate-records placement (assoc-in prefix [7 :reply :offset] 24))))
    (is (seq (fork/validate-records placement (assoc-in prefix [4 :reply :snapshot_offset] 8))))
    (is (seq (fork/validate-records placement (assoc-in prefix [4 :reply :offset] 15))))))

(deftest retries-and-deletion
  (let [retry (record "fork" "b" {:offset 14 :snapshot_offset 7} :source "a" :retry true)]
    (is (empty? (fork/validate-records :object-lease (conj prefix retry))))
    (is (seq (fork/validate-records :object-lease (conj prefix (assoc-in retry [:reply :offset] 23)))))
    (is (seq (fork/validate-records :object-lease (conj prefix (record "delete" "b" {}) retry))))))

(deftest branch-ordering-and-recovery
  (let [earlier (record "read" "b" {:offset 14 :values ["x" "y"]})]
    (is (empty? (fork/validate-records :distributed (conj prefix earlier))))
    (is (seq (fork/validate-records :object-lease (conj prefix earlier))))
    (is (seq (fork/validate-records :distributed (conj prefix (assoc-in earlier [:request :final] true)))))))

(deftest unknown-creation-resolves-through-a-matching-retry
  (let [create (nth prefix 4)
        history (h/history [{:type :invoke :f :branch-step :process 0 :value nil}
                            {:type :info :f :branch-step :process 0 :value {:request (:request create)}}
                            {:type :invoke :f :branch-step :process 0 :value nil}
                            {:type :ok :f :branch-step :process 0 :value create}])]
    (is (= [create] (vec (fork/records history))))
    (is (empty? (fork/validate-records :object-lease
                                     (concat (take 4 prefix) (fork/records history) (drop 5 prefix)))))))

(deftest missing-coverage-is-inconclusive
  (is (= :unknown (:valid? (checker/check (fork/branch-checker :object-lease)
                                         {:nodes ["n1"]} (h/history []) {})))))

(deftest a-concurrent-append-can-fall-on-either-side-of-the-capture
  (let [setup (take 3 prefix)
        concurrent {:action "append" :key "a" :value "late" :id "overlap"}
        base {:request {:action "fork" :key "b" :source "a" :concurrent concurrent}
              :reply {:offset 14 :snapshot_offset nil :concurrent {:offset 24}}}]
    (doseq [[cutoff values] [[14 ["x" "y"]] [24 ["x" "y" "late"]]]]
      (let [capture (assoc-in base [:reply :offset] cutoff)
            read (record "read" "b" {:offset cutoff :values values})]
        (is (empty? (fork/validate-records :object-lease (concat setup [capture read]))))
        (is (seq (fork/validate-records :object-lease
                                       (concat setup [capture (assoc-in read [:reply :values] ["x"])]))))))))

(deftest snapshots-can-add-yjs-state-beyond-the-recorded-offset
  (let [records [(record "initialize" "a" {:offset 0})
                 (record "append" "a" {:offset 7} :value 1)
                 (record "compact" "a" {:offset 7} :extra 2)
                 (record "fork" "b" {:offset 7 :snapshot_offset 7} :source "a")
                 (record "read" "b" {:offset 7 :values [1 2]})]]
    (is (empty? (fork/validate-records :object-lease records)))
    (is (seq (fork/validate-records :object-lease (assoc-in records [4 :reply :values] [1]))))))

(deftest retained-read-coverage-requires-deleted-ancestors
  (let [captured (take 5 prefix)
        read (record "read" "b" {:offset 14 :values ["x" "y"]} :retained true)]
    (is (zero? (:retained-reads (fork/observed-coverage (conj (vec captured) read)))))
    (is (= 1 (:retained-reads (fork/observed-coverage
                               (concat captured [(record "delete" "a" {}) read])))))))

(deftest pending-access-is-retryable-and-exhausted-retries-are-not
  (doseq [[code action retryable] [["busy" "initialize" true] ["busy" "fork" true]
                                   ["retries_exhausted" "fork" false]]]
    (with-redefs [http/post (fn [& _] {:status 409 :body (str "{\"error\":{\"code\":\"" code "\"}}")})
                  docker/ip (constantly "127.0.0.1")]
      (let [error (try (fork/request! "n1" :log {:action action})
                       (catch clojure.lang.ExceptionInfo e (ex-data e)))]
        (is (= retryable (:retryable error)))))))

(defn scenario-state []
  (atom {:next-id 0, :actors {}
         :scenarios {"0-0" {:id "0-0", :keys ["a" "b"], :stage 0, :completed [], :representations {}
                            :script [{:action "initialize" :key "a"}
                                     {:action "fork" :key "b" :source "a"}
                                     {:action "read" :key "b"}]}}}))

(defn resolve-op [state]
  (client/invoke! (fork/->Client "n1" :log state) {} {:type :invoke, :f :branch-resolve, :process 0}))

(deftest a-rejected-fork-ends-resolution-of-its-scenario
  (let [state (scenario-state)]
    (with-redefs [db/await-ready (constantly nil)
                  fork/request! (fn [_ _ {:keys [action]}]
                                  (if (= "fork" action)
                                    (throw (ex-info "conflict" {:status 409}))
                                    {:offset 0}))]
      (let [op (resolve-op state)]
        (is (= :ok (:type op)))
        (is (= ["initialize"] (map #(get-in % [:request :action]) (:value op))))
        (is (= "fork" (get-in @state [:scenarios "0-0" :rejected :action])))))))

(deftest a-timed-out-resolution-keeps-its-finished-steps
  (let [state (scenario-state)]
    (with-redefs [db/await-ready (constantly nil)
                  fork/retry-request! (fn [f] (try (f) (catch Exception _ (throw (ex-info "branch recovery deadline" {})))))
                  fork/request! (fn [_ _ {:keys [action]}]
                                  (if (= "fork" action)
                                    (throw (ex-info "unavailable" {:status 503}))
                                    {:offset 0}))]
      (let [op (resolve-op state)]
        (is (= :info (:type op)))
        (is (= ["initialize"]
               (map #(get-in % [:request :action]) (fork/records (h/history [op])))))))))
