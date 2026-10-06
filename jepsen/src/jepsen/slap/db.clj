(ns jepsen.slap.db
  "Runs jepsen/node's release on each node. Each test run keeps its shards
  under a prefix of its own in the RustFS bucket."
  (:require [clj-http.client :as http]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]
                    [db :as db]
                    [util :as util]]
            [jepsen.control.core :as core]
            [jepsen.control.util :as cu]
            [jepsen.slap.docker :refer [ip]]))

(def dir "/opt/jepsen_node")
(def logfile "/var/log/jepsen_node.log")

(defn erlang-name
  [node]
  (str "jepsen@" (ip node)))

(defn env
  [test node]
  {:RELEASE_DISTRIBUTION "name"
   :RELEASE_NODE         (erlang-name node)
   :RELEASE_COOKIE       "jepsen"
   :JEPSEN_STORE         (str "s3://jepsen/" (:store-prefix test))
   :JEPSEN_NODES         (str/join "," (map erlang-name (:nodes test)))
   :JEPSEN_SHARDS        (:shards test)
   :JEPSEN_PLACEMENT     (name (:placement test))
   :JEPSEN_LEASE_TTL     (:lease-ttl test)
   :AWS_ENDPOINT         "http://10.47.0.2:9000"
   :AWS_ALLOW_HTTP       "true"
   :AWS_REGION           "us-east-1"
   :AWS_ACCESS_KEY_ID    "rustfsadmin"
   :AWS_SECRET_ACCESS_KEY "rustfsadmin"})

(defn start!
  "Starts the node in the background, in a session of its own. (The node is
  found and signalled by process name, so it needs no pidfile.)"
  [test node]
  (c/exec (core/env (env test node))
          :setsid :nohup (str dir "/bin/jepsen_node") :start
          :>> logfile (core/lit "2>&1 < /dev/null &")))

(defn running?
  []
  (try (c/exec :pgrep :-x "beam.smp")
       true
       (catch clojure.lang.ExceptionInfo _ false)))

(defn signal!
  "Sends a signal to the node's VM. (By process name: cu/grepkill! did not
  kill it through the Docker remote.)"
  [signal]
  (cu/signal! "beam.smp" signal))

(defn kill!
  "Kills the node's VM, and waits until it has exited."
  []
  (signal! :KILL)
  (while (running?)
    (Thread/sleep 100)))

(defn await-ready
  "Waits until every node agrees on the owners of all stream and KV shards."
  [node]
  (util/await-fn
    (fn []
      (http/get (str "http://" (ip node) ":4438/ready")
                {:socket-timeout 5000, :connection-timeout 1000}))
    {:log-message (str "Waiting for " node)
     :timeout     120000}))

(defn ready-op
  [node op]
  (let [{:keys [status body]} (http/get (str "http://" (ip node) ":4438/ready")
                                        {:socket-timeout 5000, :connection-timeout 1000,
                                         :throw-exceptions false})]
    (if (= 200 status)
      (assoc op :type :ok)
      (assoc op :type :fail, :error [status body]))))

(defn db
  []
  (reify
    db/DB
    (setup! [this test node]
      (info "Starting node" (erlang-name node))
      (start! test node)
      (await-ready node))

    (teardown! [this test node]
      (kill!)
      (c/exec :rm :-f logfile))

    db/LogFiles
    (log-files [_ _test _node]
      [logfile])

    db/Process
    (start! [_ test node]
      (start! test node))

    (kill! [_ _test _node]
      (kill!))

    db/Pause
    (pause! [_ _test _node]
      (signal! :STOP))

    (resume! [_ _test _node]
      (signal! :CONT))))
