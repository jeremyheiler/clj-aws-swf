(ns aws.auth
  (:require [clojure.string :as string]
            [clojure.data.codec.base64 :as base64]
            [aws.util :as util])
  (:import [java.net URL]
           [java.util Date]
           [javax.crypto Mac] 
           [javax.crypto.spec SecretKeySpec]))

(defn ^:private headers-to-sign
  [headers]
  (letfn [(f [[k]] (or (.startsWith k "x-amz-") (= k "host")))]
    (filter f headers)))

(defn ^:private serialize-headers
  "Converts the header map into a string."
  [headers]
  (letfn [(f [[k v]] (str k ":" v "\n"))]
    (string/join (map f (sort headers)))))

(defn digest
  "Computes the digest of the given algorithm for the given input."
  [algorithm input]
  (let [algorithm-str (name algorithm)]
    (println algorithm-str)
    (.digest (java.security.MessageDigest/getInstance algorithm-str) input)))

(defn sign
  ^bytes [^bytes data ^bytes key algorithm]
  (let [algorithm-str (str "Hmac" (string/replace (name algorithm) "-" ""))]
    (println algorithm-str)
    (let [mac (Mac/getInstance algorithm-str)]
      (.init mac (SecretKeySpec. key algorithm-str))
      (.doFinal mac data))))

(defn calculate-signature-v1
  [string-to-sign algorithm signing-key]
  (String. (base64/encode
            (sign (.getBytes string-to-sign "UTF-8")
                  (.getBytes signing-key "UTF-8")
                  algorithm))))

(defn calculate-signature-v3
  [string-to-sign algorithm signing-key]
  (String. (base64/encode
            (sign (digest algorithm (.getBytes string-to-sign))
                  (.getBytes signing-key)
                  algorithm))))

(defn ^:private auth-headers-dispatch-fn
  [opts]
  (:aws-auth-version opts))

(defmulti auth-headers #'auth-headers-dispatch-fn :default 1)

(defn ^:private serialize-resource-v1
  [url headers]
  (let [bucket (if-let [host (:host headers)]
                 (first (string/split (.getHost (URL. host)) "."))
                 "")]
    (str bucket (.getPath (URL. url)))))

(defmethod auth-headers 1
  [{:keys [method url headers aws-access-key-id aws-secret-key aws-date]}]
  (let [s (string/join "\n" [(string/upper-case (name method))
                             "" ;; TODO content-md5
                             "" ;; TODO content-type
                             aws-date
                             (str (serialize-headers (headers-to-sign headers))
                                  (serialize-resource-v1 url headers))])
        signature (calculate-signature-v1 s :SHA-1 aws-secret-key)
        auth-val (str "AWS " aws-access-key-id ":" signature)]
    (println (str "String To Sign:\n" s))
    (println "Header:" auth-val)
    {"authorization" auth-val "date" aws-date}))

(defmethod auth-headers 3
  [{:keys [method url headers body aws-access-key-id aws-secret-key aws-date]}]
  (let [signable-headers (headers-to-sign (assoc headers "x-amz-date" aws-date))
        s (string/join "\n" [(string/upper-case (name method))
                             (.getPath (URL. url))
                             "" ;; TODO query string
                             (serialize-headers signable-headers)
                             body])
        signature (calculate-signature-v3 s :SHA-256 aws-secret-key)
        auth-val (str "AWS3 AWSAccessKeyId=" aws-access-key-id ","
                      "Algorithm=HmacSHA256,"
                      "SignedHeaders=" (string/join ";" (keys signable-headers)) ","
                      "Signature=" signature)]
    (println (str "String To Sign:\n" s))
    (println "Header:" auth-val)
    {"x-amzn-authorization" auth-val "x-amz-date" aws-date}))

(defn wrap-aws-auth
  "Middleware for authenticating an AWS request.

  Additional keys used by this middleware:
    :aws-auth-version
    :aws-access-key-id
    :aws-secret-key
  "
  [client]
  (fn [{:keys [aws-access-key-id aws-secret-key] :as req}]
    (if (and aws-access-key-id aws-secret-key)
      (let [req (assoc req :aws-date (util/format-date (Date.)))]
        (client (update-in req [:headers] merge (auth-headers req))))
      (client req))))
