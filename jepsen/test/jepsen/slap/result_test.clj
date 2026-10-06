(ns jepsen.slap.result-test
  (:require [clojure.test :refer [deftest is]]
            [clj-http.client :as http]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gen-test]
            [jepsen.slap.append :as append]
            [jepsen.slap.files :as files]
            [jepsen.slap.kv :as kv]
            [jepsen.slap.list :as list]
            [jepsen.slap.object-store :as object-store]
            [jepsen.slap.register :as register]
            [jepsen.slap.snapshot-log :as snapshot-log]
            [jepsen.slap.yjs :as yjs]
            [jepsen.slap :as slap]))

(deftest rejected-unconditional-writes
  (with-redefs [files/write! (fn [& _] false)
                kv/put! (fn [& _] false)]
    (is (= :fail (:type (client/invoke! (files/->Client "n1") {}
                                        {:type :invoke, :f :write, :value [0 1]}))))
    (is (= :fail (:type (client/invoke! (kv/->Client "n1") {}
                                        {:type :invoke, :f :write, :value [0 1]}))))))

(deftest cas-read-failure-did-not-write
  (with-redefs [kv/fetch (fn [_] (throw (ex-info "unavailable" {:type :http, :status 503})))
                files/fetch (fn [_] (throw (ex-info "unavailable" {:type :http, :status 503})))]
    (is (= :fail (:type (client/invoke! (kv/->Client "n1") {}
                                        {:type :invoke, :f :cas, :value [0 [1 2]]}))))
    (is (= :fail (:type (client/invoke! (files/->Client "n1") {}
                                        {:type :invoke, :f :cas, :value [0 [1 2]]}))))))

(deftest corrupt-file-cas-is-a-failed-read
  (with-redefs [files/fetch (fn [_] {:value :corrupt, :etag "1"})]
    (let [op (client/invoke! (files/->Client "n1") {}
                             {:type :invoke, :f :cas, :value [0 [1 2]]})]
      (is (= :fail (:type op)))
      (is (= :corrupt (get-in op [:error :type]))))))

(deftest non-ascii-file-digit-is-corrupt
  (is (= :corrupt (files/value "٢"))))

(deftest file-writes-have-distinct-ids
  (let [ids (atom [])]
    (with-redefs [http/put (fn [_ options]
                             (swap! ids conj (get-in options [:headers "X-Jepsen-Write-Id"]))
                             {:status 204})]
      (files/write! "http://example/files/0" 0 {})
      (files/write! "http://example/files/0" 0 {}))
    (is (every? string? @ids))
    (is (= 2 (count (set @ids))))))

(deftest malformed-kv-value-is-a-failure
  (doseq [body ["bad" "01" "5"]]
    (with-redefs [http/get (fn [& _] {:status 200, :headers {"etag" "1"}, :body body})]
      (let [op (client/invoke! (kv/->Client "n1") {}
                               {:type :invoke, :f :read, :value [0 nil]})]
        (is (= :fail (:type op)))
        (is (= :corrupt (get-in op [:error :type])))
        (is (= false (:valid? (checker/check (kv/corruption-checker) {} [op] {}))))))))

(deftest write-response-classification
  (doseq [[status expected] [[200 :ok] [504 :info]]]
    (with-redefs [http/post (fn [& _] {:status status, :body ""})]
      (is (= expected (:type (client/invoke! (yjs/->Client "n1") {}
                                              {:type :invoke, :f :add, :value 1}))))))
  (with-redefs [http/post (fn [& _] {:status 503, :body "unavailable"})]
    (is (= :fail (:type (client/invoke! (yjs/->Client "n1") {}
                                        {:type :invoke, :f :add, :value 1}))))
    (is (= :info (:type (client/invoke! (snapshot-log/->Client "n1") {}
                                        {:type :invoke, :f :txn,
                                         :value [[:append 0 1]]}))))
    (is (= :info (:type (client/invoke! (append/->Client "n1") {}
                                        {:type :invoke, :f :txn,
                                         :value [[:append 0 1]]}))))))

(deftest yjs-client-keeps-array-shape
  (with-redefs [http/get (fn [& _] {:status 200, :body "[\"2\",\"1\",\"1\"]"})]
    (is (= [2 1 1] (:value (client/invoke! (yjs/->Client "n1") {}
                                              {:type :invoke, :f :read}))))))

(deftest incomplete-follower-check
  (let [restore {:type :ok, :f :restore}]
    (doseq [result [{:mismatches [], :followers 1, :unreachable ["jepsen@n2"],
                   :stats {:log_snapshots 1}}
                  {:mismatches [], :followers 1, :read_errors [{:key "0"}],
                   :stats {:log_snapshots 1}}
                  {:mismatches [], :followers 1,
                   :stats {:log_snapshots 1, :log_superseded 0, :log_resets 0}}]]
      (is (= :unknown
             (:valid? (checker/check (snapshot-log/followers-checker) {}
                                     [restore {:type :ok, :f :check, :value result}] {})))))))

(deftest follower-coverage-and-catch-up
  (let [base {:nodes 2, :keys 1, :followers 2, :missing [], :lagging [],
              :mismatches [], :read_errors [], :unreachable [],
              :stats {:log_snapshots 1, :log_resets 1}}
        check (fn [result]
                (:valid? (checker/check (snapshot-log/followers-checker) {}
                                        [{:type :ok, :f :restore}
                                         {:type :ok, :f :check, :value result}] {})))]
    (is (= true (check base)))
    (is (= :unknown (:valid? (checker/check (snapshot-log/followers-checker) {}
                                           [{:type :ok, :f :check, :value base}] {}))))
    (is (= :unknown (check (assoc base :missing ["0"] :followers 1))))
    (is (= :unknown (check (assoc base :lagging [{:key "0"}]))))
    (is (= false (check (assoc base :mismatches [{:key "0"}]))))
    (is (= true (check (-> base
                           (assoc :stats {:log_snapshots 2})
                           (assoc :publication_nodes {"0" ["n1" "n2"]})))))
    (is (= :unknown (check (-> base
                               (assoc :stats {:log_snapshots 2})
                               (assoc :publication_nodes {"0" ["n1"]})))))))

(deftest yjs-array-decisions
  (let [test {:nodes ["n1" "n2"]}
        base [{:type :invoke, :f :add, :value 1}
              {:type :ok, :f :add, :value 1}
              {:type :invoke, :f :add, :value 2}
              {:type :ok, :f :add, :value 2}
              {:type :invoke, :f :read, :final true, :process 0}
              {:type :ok, :f :read, :final true, :process 0, :value [1 2]}
              {:type :invoke, :f :read, :final true, :process 1}
              {:type :ok, :f :read, :final true, :process 1, :value [1 2]}]
        check (fn [history]
                (:valid? (checker/check (yjs/array-checker) test history {})))]
    (is (= true (check base)))
    (is (= false (check (assoc-in base [5 :value] [1 1 2]))))
    (is (= false (check (assoc-in base [7 :value] [2 1]))))
    (is (= false (check (assoc-in base [7 :value] [1]))))
    (is (= :unknown (check (assoc-in base [7 :type] :fail))))
    (is (= :unknown (check (pop base))))))

(deftest list-final-reads-must-cover-every-node-and-key
  (let [test {:nodes ["n1" "n2"]}
        base [{:type :invoke, :f :txn, :value [[:append 3 1]]}
              {:type :ok, :f :txn, :value [[:append 3 1]]}
              {:type :invoke, :f :txn, :final true, :process 0, :value [[:r 3 nil]]}
              {:type :ok, :f :txn, :final true, :process 0, :value [[:r 3 [1]]]}
              {:type :invoke, :f :txn, :final true, :process 1, :value [[:r 3 nil]]}
              {:type :ok, :f :txn, :final true, :process 1, :value [[:r 3 [1]]]}]
        check (fn [history]
                (:valid? (checker/check (list/final-read-checker) test history {})))]
    (is (= true (check base)))
    (is (= :unknown (check (assoc-in base [5 :type] :fail))))
    (is (= :unknown (check (subvec (vec base) 0 4))))))

(deftest fault-coverage-requires-completed-fault-and-recovery
  (let [test [{:index 0, :process :nemesis, :type :info,
               :f :start-partition, :value :one}
              {:index 1, :process :nemesis, :type :info,
               :f :start-partition, :value [:isolated {"n1" #{"n2"}}]}
              {:index 2, :process :nemesis, :type :info,
               :f :stop-partition, :value :network-healed}]
        check (fn [history]
                (:valid? (checker/check (slap/fault-checker #{:partition}) {}
                                        history {})))]
    (is (= true (check test)))
    (is (= :unknown (check (pop test))))
    (is (= :unknown (check (subvec (vec test) 0 1))))
    (is (= true (:valid? (checker/check (slap/fault-checker #{:object-store}) {}
                                        [{:index 0, :process :nemesis, :type :info,
                                          :f :pause-store, :value :paused}
                                         {:index 1, :process :nemesis, :type :info,
                                          :f :resume-store, :value :resumed}] {}))))
    (is (= true (:valid? (checker/check (slap/fault-checker #{:kill}) {}
                                        [{:index 0, :process :nemesis, :type :info,
                                          :f :kill, :value {"n1" nil}}
                                         {:index 1, :process :nemesis, :type :info,
                                          :f :start, :value {"n1" ""}}] {}))))
    (is (= :unknown (:valid? (checker/check (slap/fault-checker #{:pause}) {}
                                            [{:index 0, :process :nemesis, :type :info,
                                              :f :pause, :value {"n1" :failed}}
                                             {:index 1, :process :nemesis, :type :info,
                                              :f :resume, :value {"n1" :signaled}}] {}))))))

(deftest object-store-faults-repeat
  (let [faults (:generator (object-store/package {:faults #{:object-store}, :interval 10}))
        ops (filter #(= :info (:type %))
                    (gen-test/quick-ops (gen/limit 6 faults)))]
    (is (= [:pause-store :resume-store :pause-store :resume-store
            :pause-store :resume-store]
           (mapv :f ops)))
    (is (every? #(>= % 20e9)
                (map - (map :time (rest ops)) (map :time ops))))))

(deftest missing-final-read
  (let [history [{:type :invoke, :f :write, :value [0 1]}
                 {:type :ok, :f :write, :value [0 1]}
                 {:type :ok, :f :ready}]]
    (is (= :unknown (:valid? (checker/check (register/final-read-checker)
                                           {:nodes ["n1"]} history {}))))
    (is (= true (:valid? (checker/check (register/final-read-checker) {:nodes ["n1"]}
                                       (into history [{:type :invoke, :f :read, :final true,
                                                       :process 0, :value [0 nil]}
                                                      {:type :ok, :f :read, :final true,
                                                       :process 0, :value [0 1]}]) {}))))))

(deftest every-client-must-complete-its-final-read
  (let [history [{:type :invoke, :f :write, :process 0, :value [0 1]}
                 {:type :ok, :f :write, :process 0, :value [0 1]}
                 {:type :ok, :f :ready, :process 0}
                 {:type :invoke, :f :read, :final true, :process 0, :value [0 nil]}
                 {:type :invoke, :f :read, :final true, :process 1, :value [0 nil]}
                 {:type :ok, :f :read, :final true, :process 0, :value [0 1]}
                 {:type :fail, :f :read, :final true, :process 1, :value [0 nil]}]]
    (is (= :unknown (:valid? (checker/check (register/final-read-checker) {} history {}))))
    (is (= :unknown (:valid? (checker/check (register/final-read-checker) {}
                                       (pop history) {}))))))

(deftest every-client-must-schedule-every-key
  (let [test {:nodes ["n1" "n2"]}
        history [{:type :invoke, :f :write, :process 0, :value [0 1]}
                 {:type :ok, :f :write, :process 0, :value [0 1]}
                 {:type :ok, :f :ready, :process 0}
                 {:type :invoke, :f :read, :final true, :process 0, :value [0 nil]}
                 {:type :ok, :f :read, :final true, :process 0, :value [0 1]}]]
    (is (= :unknown (:valid? (checker/check (register/final-read-checker)
                                           test history {}))))))

(deftest missing-file-body-storage-mode
  (let [audit {:quiescent true, :dangling [], :orphans [], :leaked_registrations [],
               :intents 0, :unreconciled [],
               :stats {:files_inline_writes 1, :files_object_writes 0}}]
    (is (= :unknown (:valid? (checker/check (files/audit-checker) {}
                                       [{:type :ok, :f :audit, :value audit}] {}))))))
