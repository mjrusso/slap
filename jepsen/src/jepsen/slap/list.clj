(ns jepsen.slap.list
  (:require [clojure.set :as set]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]))

(defn track-keys
  [keys generator]
  (gen/map (fn [op]
             (when (and (= :invoke (:type op)) (= :txn (:f op)))
               (swap! keys into (map second (:value op))))
             op)
           generator))

(defn final-generator
  [keys]
  (gen/each-thread
    (delay (map (fn [k] {:f :txn, :final true, :value [[:r k nil]]})
                (sort @keys)))))

(defn final-read-checker
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [touched (set (for [op history
                            :when (and (= :invoke (:type op)) (= :txn (:f op)))
                            [_ k _] (:value op)]
                        k))
            reads (filter #(and (:final %) (= :txn (:f %))) history)
            id (juxt :process (comp second first :value))
            scheduled (frequencies (map id (filter #(= :invoke (:type %)) reads)))
            completed (frequencies (map id (filter #(= :ok (:type %)) reads)))
            processes (set (map first (keys scheduled)))
            covered? (every? (fn [process]
                               (= touched (set (for [[p k] (keys scheduled)
                                                     :when (= p process)] k))))
                             processes)
            expected (* (count (:nodes test)) (count touched))]
        {:valid? (if (and (seq touched)
                          (= expected (count scheduled))
                          (= (count (:nodes test)) (count processes))
                          covered?
                          (every? #(= 1 %) (vals scheduled))
                          (= scheduled completed)) true :unknown)
         :keys (count touched)
         :scheduled (count scheduled)
         :completed (count completed)
         :incomplete (set/difference (set (keys scheduled)) (set (keys completed)))}))))
