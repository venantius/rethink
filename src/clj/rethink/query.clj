(ns rethink.query
  (:refer-clojure :exclude [and filter fn make-array mod not or update var])
  (:require [clojure.walk :as walk]
            [rethink.engine :as engine]
            [rethink.java :as java]))

(defonce conn (java/connect {}))

(defn run
  [reql-ast conn]
  (engine/run reql-ast conn))

;; utility

(defn- first-term-type
  [& x] (:term-type (first x)))

(defn reql-ast
  "Generate a Reql Ast map"
  [term-type args optargs]
  {:term-type term-type
   :args args
   :optargs optargs})

;; Manipulating databases

(defn db-create
  "Create a database. A RethinkDB database is a collection of tables, similar to
  relational databases."
  [dbname]
  (reql-ast :DB_CREATE [dbname] nil))

(defn db-drop
  "Drop a database. The database, all its tables, and corresponding data will be
  deleted."
  [dbname]
  (reql-ast :DB_DROP [dbname] nil))

(defn db-list
  "List all database names in the cluster. The result is a list of strings."
  []
  (reql-ast :DB_LIST [] nil))

;; Manipulating tables

(defmulti table-create
  first-term-type)

(defmethod table-create :DB
  [db tablename & optargs]
  (reql-ast :TABLE_CREATE [db tablename] (first optargs)))

(defmethod table-create :default
  [tablename & optargs]
  (reql-ast :TABLE_CREATE [tablename] (first optargs)))

(defmulti table-drop
  {:arglists '([tablename & optargs] [db tablename & optargs])}
  first-term-type)

(defmethod table-drop :DB
  [db tablename & optargs]
  (reql-ast :TABLE_DROP [db tablename] optargs))

(defmethod table-drop :default
  [tablename & optargs]
  (reql-ast :TABLE_DROP [tablename] optargs))

(defmulti table-list
  {:arglists '([& optargs] [db & optargs])}
  first-term-type)

(defmethod table-list :DB
  [db & optargs]
  (reql-ast :TABLE_LIST [db] optargs))

(defmethod table-list :default
  [& optargs]
  (reql-ast :TABLE_LIST [] optargs))

;; TODO: index-create
;; TODO: index-drop
;; TODO: index-list
;; TODO: index-rename
;; TODO: index-status
;; TODO: index-wait


;; Writing data

(defn insert
  "Insert documents into a table. Accepts a single document or an array of
  documents."
  [table data]
  (reql-ast :INSERT [table data] nil))

;; TODO
(defn update
  [query object-or-function])

;; Selecting data

(defn db
  [args]
  (reql-ast :DB [args] nil))

;; TODO: allow to pass query (i.e. reql-ast :DB)
(defmulti table
  {:arglists '([tablename] [db tablename])}
  first-term-type)

(defmethod table :DB
  [db tablename]
  (reql-ast :TABLE [tablename] nil))

(defmethod table :default
  [tablename]
  (reql-ast :TABLE [tablename] nil))

(defn filter
  [query pred-fn]
  (reql-ast :FILTER [query pred-fn] nil))

;; Document manipulation

(defn get-field
  [query field]
  (reql-ast :GET_FIELD [query field] nil))

;; common alias
(def g get-field)

;; Math and logic


(defn add
  "Sum two or more numbers, or concatenate two or more strings or arrays.

  The add command can be called in either prefix or infix form; both forms are
  equivalent. Note that ReQL will not perform type coercion. You cannot, for
  example, add a string and a number together."
  [value1 value2 & values]
  (reql-ast :ADD (into [] (concat [value1 value2] values)) nil))

(defn sub
  "Subtract two numbers."
  [value & values]
  (reql-ast :SUB (into [] (concat [value] values)) nil))

(defn mul
  "Multiply two numbers, or make a periodic array."
  [value & values]
  (reql-ast :MUL (into [] (concat [value] values)) nil))

(defn div
  "Divide two numbers."
  [value & values]
  (reql-ast :DIV (into [] (concat [value] values)) nil))

(defn mod
  "Find the remainder when dividing two numbers."
  [value1 value2]
  (reql-ast :MOD [value1 value2] nil))

(defn and
  "Compute the logical “and” of one or more values.

  Calling and with zero arguments will return true."
  [& values]
  (reql-ast :AND (into [] values) nil))

(defn or
  "Compute the logical “or” of one or more values.

  Calling or with zero arguments will return false."
  [& values]
  (reql-ast :OR (into [] values) nil))

(defn eq
  "Test if two or more values are equal."
  [value1 value2 & values]
  (reql-ast :EQ (into [] (concat [value1 value2] values)) nil))

(defn ne
  "Test if two or more values are not equal."
  [value & values]
  (reql-ast :NE (into [] (concat [value] values)) nil))

(defn gt
  "Compare values, testing if the left-hand value is greater than the right-hand."
  [value & values]
  (reql-ast :GT (into [] (concat [value] values)) nil))

(defn ge
  "Compare values, testing if the left-hand value is greater than or equal to
  the right-hand."
  [value & values]
  (reql-ast :GE (into [] (concat [value] values)) nil))

(defn lt
  "Compare values, testing if the left-hand value is less than the right-hand."
  [value & values]
  (reql-ast :LT (into [] (concat [value] values)) nil))

(defn le
  "Compare values, testing if the left-hand value is less than or equal to the
  right-hand."
  [value & values]
  (reql-ast :LE (into [] (concat [value] values)) nil))

(defn not
  "Compute the logical inverse (not) of an expression.

  All values that are not false or null will be converted to true."
  [value]
  (reql-ast :NOT [value] nil))

(defn random
  "Generate a random number between given (or implied) bounds. random takes
  zero, one or two arguments, and can also take an optarg of float."
  [& args]
  (if (map? (last args))
    (reql-ast :RANDOM (into [] (drop-last args)) (last args))
    (reql-ast :RANDOM (into [] args) nil)))

(defn round
  "Rounds the given value to the nearest whole integer.

  For example, values of 1.0 up to but not including 1.5 will return 1.0,
  similar to floor; values of 1.5 up to 2.0 will return 2.0, similar to ceil."
  [value]
  (reql-ast :ROUND [value] nil))

(defn ceil
  "Rounds the given value up, returning the smallest integer greater than or
  equal to the given value (the value’s ceiling)."
  [value]
  (reql-ast :CEIL [value] nil))

(defn floor
  "Rounds the given value down, returning the largest integer value less than
  or equal to the given value (the value’s floor)."
  [value]
  (reql-ast :FLOOR [value] nil))

;; Control structures

;; Special

(defn datum
  [arg]
  (reql-ast :DATUM [arg] nil))

(defn make-array
  [& args]
  (reql-ast :MAKE_ARRAY (into [] (flatten args)) nil))

(defn func
  "args are the actual arguments to the function"
  [args terms]
  (reql-ast :FUNC [args terms] nil))

(defn funcall
  [f & args]
  (reql-ast :FUNCALL (into [] (concat [f] args)) nil))

(defn var
  [args]
  (reql-ast :VAR [args] nil))

(defmacro fn [args & [body]]
  (let [new-args (map #(. Integer valueOf %)
                      (take (count args)
                            (repeatedly (comp str (partial gensym "")))))
        new-replacements (zipmap args (map var new-args))
        new-terms (walk/postwalk-replace new-replacements body)]
    (func (make-array new-args) new-terms)))
