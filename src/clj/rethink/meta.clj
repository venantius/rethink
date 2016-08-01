(ns rethink.meta
  "Metaprogramming for profit."
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]))

(def term-info
  (json/parse-string
   (slurp
    (io/file
     (io/resource "term_info.json")))
   true))

(defn fn-name-from-type-type
  "Turns a term-type, e.g. `TABLE_CREATE` into a clojuresque fn name like
  `table-create`."
  [term-type]
  (-> term-type
      name
      clojure.string/lower-case
      (clojure.string/replace #"_" "-")
      symbol))

(def signatures
  {"T_EXPR" 'expr
   "T_FUNC0" 'fn
   "T_FUNC1" 'fn
   "T_FUNC2" 'fn
   "T_FUNC3" 'fn
   "T_DB" 'db})

(defn generate-fn-body
  [signature]
  (mapv #(get signatures %) signature))

(defn generate-function
  "Given a term-type corresponding to an entry in the map generated from parsing
  term_info.json, construct a function for that type."
  [term-type]
  (let [{:keys [id include_in optargs side_effect signatures]} (term-type term-info)
        fn-name (fn-name-from-type-type term-type)]
    (term-type term-info)
    `(fn ~fn-name ~(map generate-fn-body signatures))))
