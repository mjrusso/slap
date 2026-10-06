(ns jepsen.slap.register
  (:require [clojure.set :as set]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.independent :as independent]))

(defn track-keys
  [keys generator]
  (gen/map (fn [op]
             (when (= :invoke (:type op))
               (swap! keys conj (first (:value op))))
             op)
           generator))

(defn final-generator
  [keys]
  (gen/phases
    (gen/time-limit 60 (gen/until-ok (gen/stagger 1 (repeat {:f :ready}))))
    (gen/each-thread
      (delay (map (fn [k] {:f :read, :final true,
                          :value (independent/tuple k nil)})
                  (sort @keys))))))

(defn final-read-checker
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [touched (set (for [op history
                            :when (= :invoke (:type op))
                            :when (#{:read :write :cas} (:f op))]
                        (first (:value op))))
            final-reads (filter #(and (:final %) (= :read (:f %))) history)
            target (juxt :process (comp first :value))
            scheduled (frequencies (map target (filter #(= :invoke (:type %)) final-reads)))
            completed (frequencies (map target (filter #(= :ok (:type %)) final-reads)))
            processes (set (map first (keys scheduled)))
            covered? (every? (fn [process]
                               (= touched (set (for [[p k] (keys scheduled)
                                                     :when (= p process)] k))))
                             processes)
            ready? (some #(and (= :ready (:f %)) (= :ok (:type %))) history)]
        {:valid? (if (and ready? (seq touched)
                          (= (count (:nodes test)) (count processes))
                          (= (* (count (:nodes test)) (count touched)) (count scheduled))
                          covered?
                          (every? #(= 1 %) (vals scheduled))
                          (= scheduled completed)) true :unknown)
         :missing (sort (set/difference touched
                                        (set (map (comp second target)
                                                  (filter #(= :ok (:type %)) final-reads)))))
         :incomplete (into {}
                           (keep (fn [[client-key count]]
                                   (let [remaining (- count (get completed client-key 0))]
                                     (when (pos? remaining) [client-key remaining]))))
                           scheduled)}))))
