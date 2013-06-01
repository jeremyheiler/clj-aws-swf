(ns aws.swf
  "Defines all the SWF actions and workers."
  (:require [clojure.string :as string]
            [clojure.data.codec.base64 :as base64]
            [aws.http :as http]
            [aws.util :as util]
            [aws.runner :as runner]
            [cheshire.core :as json]))

;; TODO provide the following defs as a config map?

(def host "swf.us-east-1.amazonaws.com")
(def endpoint (str "https://" host "/"))
(def target-base "SimpleWorkflowService.")
;(def target-base "com.amazonaws.swf.service.model.SimpleWorkflowService.")

(defn ^:private create-headers
  [host action]
  {"host" host
   "x-amz-target" (str target-base (util/camel-case action :type :capitalize))
   "content-type" "application/x-amz-json-1.0"})

(defn perform-action
  "Performs the action by sending a request to the SWF API."
  ([action payload]
     (perform-action action payload {}))
  ([action payload {:keys [access-key-id secret-key socket-timeout]}]
     (let [opts {:method :post
                 :url endpoint
                 :headers (create-headers host action)
                 :body (json/encode payload {:key-fn util/encode-key})
                 :aws-auth-version 3
                 :aws-access-key-id access-key-id
                 :aws-secret-key secret-key
                 :socket-timeout socket-timeout}]
       (json/decode (:body (http/request opts)) util/decode-key))))

(defmacro defaction
  "Defines a function that will perform the specified action by
  submitting an HTTP request to Amazon. The function will be stored in
  a var with the given name, and excepts a map for the actions payload
  as its only argument.

  Recognized options:
  :socket-timeout sets the HTTP connection's socket timeout in
    milliseconds.
  "
  [name payload-keys & opts]
  `(defn ~name
     [payload#]
     (let [opts# (apply hash-map ~opts)]
       (perform-action
        ~(keyword name)
        (select-keys payload# ~payload-keys)
        (if (empty? (select-keys opts# [:access-key-id :secret-key]))
          (assoc opts#
            :access-key-id (System/getenv "AWS_ACCESS_KEY_ID")
            :secret-key (System/getenv "AWS_SECRET_KEY"))
          opts#)))))


(defaction count-closed-workflow-executions
  [:close-status-filter
   :close-time-filter
   :domain
   :execution-filter
   :start-time-filter
   :tag-filter
   :type-filter])

(defaction count-open-workflow-executions
  [:domain
   :execution-filter
   :start-time-filter
   :tag-filter
   :type-filter])

(defaction count-pending-activity-tasks
  [:domain
   :task-list])

(defaction count-pending-decision-tasks
  [:domain
   :task-list])

(defaction deprecate-activity-type
  [:activity-type
   :domain])

(defaction deprecate-domain
  [:name])

(defaction deprecate-workflow-type
  [:domain
   :workflow-type])

(defaction describe-activity
  [:activity-type
   :domain])

(defaction describe-domain
  [:name])

(defaction describe-workflow-execution
  [:domain
   :execution])

(defaction describe-workflow-type
  [:domain
   :workflow-type])

(defaction get-workflow-execution-history
  [:domain
   :execution
   :maximum-page-size
   :next-page-token
   :reverse-order])

(defaction list-activity-types
  [:domain
   :maximum-page-size
   :name
   :next-page-token
   :registration-status
   :reverse-order])

(defaction list-closed-workflow-executions
  [:close-status-filter
   :close-time-filter
   :domain
   :execution-filter
   :maximum-page-size
   :next-page-token
   :reverse-order
   :start-time-filter
   :tag-filter
   :type-filter])

(defaction list-domains
  [:maximum-page-size
   :next-page-token
   :registration-status
   :reverse-order])

(defaction list-open-workflow-executions
  [:domain
   :execution-filter
   :maximum-page-size
   :next-page-token
   :reverse-order
   :start-time-filter
   :tag-filter
   :type-filter])

(defaction list-workflow-types
  [:domain
   :maximum-page-size
   :name
   :next-page-token
   :registration-status
   :reverse-order])

(defaction poll-for-activity-task
  [:domain
   :identity
   :task-list]
  :socket-timeout 70000)

(defaction poll-for-decision-task
  [:domain
   :task-list
   :identity]
  :socket-timeout 70000)

(defaction record-activity-task-heartbeat
  [:detials
   :task-token])

(defaction register-activity-type
  [:default-task-heartbeat-timeout
   :default-task-list
   :default-task-schedule-to-close-timeout
   :default-task-schedule-to-start-timeout
   :default-task-start-to-close-timeout
   :description
   :domain
   :name
   :version])

(defaction register-domain
  [:description
   :name
   :workflow-execution-retention-period-in-days])

(defaction register-workflow-type
  [:default-child-policy
   :default-execution-start-to-close-timeout
   :default-task-list
   :default-task-start-to-close-timeout
   :description
   :domain
   :name
   :version])

(defaction request-cancel-workflow-execution
  [:domain
   :run-id
   :workflow-id])

(defaction respond-activity-task-canceled
  [:details
   :task-token])

(defaction respond-activity-task-completed
  [:result
   :task-token])

(defaction respond-activity-task-failed
  [:details
   :reason
   :task-token])

(defaction respond-decision-task-completed
  [:decisions
   :execution-context
   :task-token])

(defaction signal-workflow-execution
  [:domain
   :input
   :run-id
   :signal-name
   :workflow-id])

(defaction start-workflow-execution
  [:child-policy
   :domain
   :execution-start-to-close-timeout
   :input
   :tag-list
   :task-list
   :task-start-to-close-timeout
   :workflow-id
   :workflow-type])

(defaction terminate-workflow-execution
  [:child-policy
   :details
   :domain
   :reason
   :run-id :workflow-id])

;; TODO Stopping a worker does not cancel a future that has started.
;;      Perhaps have a force-stop or a 'force' parameter to the stop
;;      method?

;; TODO It appears that the heartbeat timeout is application-specific,
;;      so there needs to be a way for it to be configurable.

;; TODO I'm curious about how future-cancel works.

(defn ^:private extract-ex
  [e]
  (clojure.stacktrace/print-stack-trace e)
  (print (ex-data e))
  {:details (prn-str (ex-data e))
   :reason (.getMessage e)})

(defmacro ^:private maybe-ex
  "Returns the result of the body or, if an exception occurs, the
  exception object."
  [& body]
  `(try
     ~@body
     (catch Exception e#
       e#)))

(defn ^:private do-activity-loop
  [activity args]
  (loop []
    (let [result (deref activity 30000 :heartbeat)]
      (if-not (= :heartbeat result)
        (if (instance? Exception result)
          (respond-activity-task-failed (merge args (extract-ex result)))
          (respond-activity-task-completed (assoc args :result result)))
        (if (:cancel-requested (record-activity-task-heartbeat args))
          (do
            (future-cancel activity)
            (respond-activity-task-canceled args))
          (recur))))))

(defn do-activity
  "Polls for an ActivityTask and processes it if one is given.

  The activity-fn must accept one argument which is the ActivityTask
  map. The optional args will be passed into the PollForActivityTask
  action, so be sure to include all the necessary parameters."
  [activity-fn & [args]]
  (let [task (poll-for-activity-task args)]
    (when-let [token (:task-token task)]
      (let [input {:task-token token}]
        (try
          (let [activity (future (maybe-ex (activity-fn task)))]
            (do-activity-loop activity input))
          (catch Exception e
            (respond-activity-task-failed (merge input (extract-ex e)))))))))

(defn do-decision
  "Polls for a DecisionTask and processes it if one is given.

  The decision-fn must accept one argument which is the DecisionTask
  map. The optional args will be passed into the PollForDecisionTask
  action, so be sure to include all the ncessary parameters."
  [decision-fn & [args]]
  (let [task (poll-for-decision-task args)]
    (when-let [token (:task-token task)]
      (-> (decision-fn task)
          (assoc :task-token token)
          (respond-decision-task-completed)))))

(defmacro defworker
  "The args vector must have only one argument, which will be the
  ActivtyTask map. You must provide it so you can have access to it,
  and potentially destructure it to fit your needs. The value returned
  from the worker must be a string and it will be sent as the 'result'
  value."
  [name args & body]
  `(def ~name (runner/runner (partial do-activity (fn ~args ~@body)) identity)))

(defmacro defdecider
  "The args vector must have only one argument, which will be the
  DecisionTask map. You must provide it so you can have access to it,
  and potentially destructure it to fit your needs."
  [name args & body]
  `(def ~name (runner/runner (partial do-decision (fn ~args ~@body)) identity)))
