(ns aws.http
  (:require [aws.auth :as auth]
            [clj-http.client :as http]))

(defn request
  ([method url & [opts]]
     (request (assoc opts :url url :method method)))
  ([opts]
     (http/with-middleware (conj http/default-middleware #'auth/wrap-aws-auth)
       (http/request opts))))
