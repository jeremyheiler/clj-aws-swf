(ns aws.util
  (:require [clojure.string :as string])
  (:import [java.text SimpleDateFormat]))

(defn format-date
  [date]
  (.format (SimpleDateFormat. "EEE, dd MMM yyyy HH:mm:ss zzz") date))

(defn uncapitalize
  "Converts the first character of the string to lower-case."
  [s]
  (apply str (string/lower-case (first s)) (rest s)))

(defn camel-case
  "Converts a dash case keyword to a camel case string."
  [k & {:keys [type] :or {type :capitalize}}]
  (let [s (string/join (map string/capitalize (string/split (name k) #"-")))]
    (if (= type :uncapitalize) (uncapitalize s) s)))

(defn camelize
  ([k] (camelize k :capitalize))
  ([k type] (camel-case k :type type)))

(defn decode-key
  "Converts keys from camel case strings to lower dash case keywords."
  [key]
  (-> key
      (.replaceAll "([A-Z]+)([A-Z][a-z])" "$1-$2")
      (.replaceAll "([a-z\\d])([A-Z])" "$1-$2")
      (string/lower-case)
      (keyword)))

(defn encode-key
  "Converts keys from lower dash case keywords to camel case strings."
  [key]
  (-> key
      (name)
      (camelize :uncapitalize)))
