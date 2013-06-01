(ns aws.runner)

(defprotocol Runner
  "A protocol for starting and stopping runners."
  (start [runner] [runner args] "Start the runner with an arg map.")
  (stop [runner] "Stop the runner."))

;; A Runner complects:
;; - Where the code runs (current thread or another)
;; - How the runner knows that it is active or not

(deftype BasicRunner [f active? error-fn]
  Runner
  (start [runner args]
    (when-not @active?
      (reset! active? true))
    (future
      (while @active?
        (try
          (f args)
          (catch Exception e
            (error-fn e))))))
  (stop [runner]
    (reset! active? false)))

(defn runner
  "Create a basic worker that has not started."
  [f error-fn]
  (BasicRunner. f (atom false) error-fn))

