(ns jepsen.slap.docker
  "Runs Jepsen's control commands in the Compose containers (docker/compose.yml)
  with `docker exec` and `docker cp`. Node n1 is container slap-jepsen-n1-1, at
  10.47.0.11; commands run as root."
  (:require [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [jepsen.control.core :as core]
            [jepsen.control.docker :as docker]))

(defn ip
  "A node's address: n1 is 10.47.0.11."
  [node]
  (str "10.47.0." (+ 10 (parse-long (subs node 1)))))

(defn container
  [node]
  (str "slap-jepsen-" node "-1"))

(defn cp-from
  "Copies files from a container. Not `docker cp`, which takes a local path
  with a colon, as Jepsen's store paths have, for a container's."
  [container-id remote-paths local-path]
  (doseq [remote-path (flatten [remote-paths])]
    (let [{:keys [exit out err]} (sh "docker" "exec" container-id "cat" remote-path
                                     :out-enc :bytes)
          dest (io/file local-path)
          dest (if (.isDirectory dest) (io/file dest (.getName (io/file remote-path))) dest)]
      (when-not (zero? exit)
        (throw (ex-info (str "copy of " remote-path " failed: " err)
                        {:type ::copy-failed, :exit exit})))
      (io/copy out dest))))

(defrecord Remote [container-id]
  core/Remote
  (connect [this conn-spec]
    (assoc this :container-id (container (:host conn-spec))))
  (disconnect! [this]
    (assoc this :container-id nil))
  (execute! [_ _ctx action]
    (docker/exec container-id action))
  (upload! [_ _ctx local-paths remote-path _opts]
    (docker/cp-to container-id local-paths remote-path))
  (download! [_ _ctx remote-paths local-path _opts]
    (cp-from container-id remote-paths local-path)))

(def remote (->Remote nil))
