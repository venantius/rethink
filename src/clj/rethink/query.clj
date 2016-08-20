(ns rethink.query
  (:refer-clojure :exclude [and filter fn get make-array mod not or replace
                            sync update
                            var])
  (:require [clojure.walk :as walk]
            [rethink.engine :as engine]
            [rethink.java :as java]))

(defonce conn (java/connect {}))

(defn run
  [query conn]
  (engine/run query conn))

;; utility

(defn- first-term-type
  [& x] (:term-type (first x)))

(defn reql-ast
  "Generate a Reql Ast map"
  [term-type args optargs]
  {:term-type term-type
   :args args
   :optargs optargs})

(defn optargs-expr-helper
  [term & args]
  (if (map? (last args))
    (reql-ast term (into [] (drop-last args)) (last args))
    (reql-ast term (into [] args) nil)))

;; Accessing ReQL (TODO)

;; Manipulating databases (COMPLETE)

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

;; Manipulating tables (COMPLETE)

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

(defn index-create
  "Create a new secondary index on a table. Secondary indexes improve the speed
  of many read queries at the slight cost of increased storage space and
  decreased write performance."
  [table index-name & args]
  (if-let [optarg (clojure.core/and
                   (map? (last args))
                   (clojure.core/not (engine/reql-ast? (last args))))]
    (reql-ast :INDEX_CREATE (into [table index-name] (drop-last args)) (last args))
    (reql-ast :INDEX_CREATE (into [table index-name] args) nil)))

(defn index-list
  "List all the secondary indexes of this table."
  [table]
  (reql-ast :INDEX_LIST [table] nil))

(defn index-rename
  "Rename an existing secondary index on a table. If the optarg `overwrite` is
  specified as true, a previously existing index with the new name will be
  deleted and the index will be renamed. If overwrite is false (the default) an
  error will be raised if the new index name already exists."
  ([table old-index-name new-index-name]
   (reql-ast :INDEX_RENAME [table old-index-name new-index-name] nil))
  ([table old-index-name new-index-name optargs]
   (reql-ast :INDEX_RENAME [table old-index-name new-index-name] optargs)))

(defn index-status
  "Get the status of the specified indexes on this table, or the status of all
  indexes on this table if no indexes are specified."
  ([table]
   (reql-ast :INDEX_STATUS [table] nil))
  ([table index]
   (reql-ast :INDEX_STATUS [table index] nil)))

(defn index-wait
  "Wait for the specified indexes on this table to be ready, or for all indexes
  on this table to be ready if no indexes are specified."
  ([table]
   (reql-ast :INDEX_WAIT [table] nil))
  ([table index]
   (reql-ast :INDEX_WAIT [table index] nil)))

(defn index-drop
  "Delete a previously created secondary index of this table."
  [table index]
  (reql-ast :INDEX_DROP [table index] nil))

;; Writing data (COMPLETE)

(defn insert
  "Insert documents into a table. Accepts a single document or an array of
  documents."
  [table data]
  (reql-ast :INSERT [table data] nil))

(defn update
  "Update JSON documents in a table. Accepts a JSON document, a ReQL
  expression, or a combination of the two.

  You can pass the following optional arguments:

    durability: possible values are hard and soft. This option will override
                the table or query’s durability setting (set in run). In soft
                durability mode RethinkDB will acknowledge the write immediately
                after receiving it, but before the write has been committed to
                disk.
    return_changes:
      true: return a changes array consisting of old_val/new_val objects
            describing the changes made, only including the documents actually
            updated.
      false: do not return a changes array (the default).
      \"always\": behave as true, but include all documents the command tried
                  to update whether or not the update was successful. (This was
                  the behavior of true pre-2.0.)
    non_atomic: if set to true, executes the update and distributes the result
                to replicas in a non-atomic fashion. This flag is required to
                perform non-deterministic updates, such as those that require
                reading data from another table."
  ([query obj-or-fn]
   (reql-ast :UPDATE [query obj-or-fn] nil))
  ([query obj-or-fn optargs]
   (reql-ast :UPDATE [query obj-or-fn] optargs)))

(defn replace
  "Replace documents in a table. Accepts a JSON document or a ReQL expression,
  and replaces the original document with the new one. The new document must
  have the same primary key as the original document.

  The replace command can be used to both insert and delete documents. If the
  “replaced” document has a primary key that doesn’t exist in the table, the
  document will be inserted; if an existing document is replaced with null, the
  document will be deleted. Since update and replace operations are performed
  atomically, this allows atomic inserts and deletes as well."
  ([query obj-or-fn]
   (reql-ast :REPLACE [query obj-or-fn] nil))
  ([query obj-or-fn optargs]
   (reql-ast :REPLACE [query obj-or-fn] optargs)))

(defn delete
  "Delete one or more documents from a table."
  ([query]
   (reql-ast :DELETE [query] nil))
  ([query optargs]
   (reql-ast :DELETE [query] optargs)))

(defn sync
  "Ensure that writes on a given table are written to permanent storage.
  Queries that specify soft durability do not wait for writes to be committed
  to disk; a call to sync on a table will not return until all previous writes
  to the table are completed, guaranteeing the data’s persistence."
  [table]
  (reql-ast :SYNC [table] nil))

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

(defn get
  [table key]
  (reql-ast :GET [table key] nil))

(defn get-all
  [table & keys]
  (reql-ast :GET_ALL (into [table] keys) nil))

;; TODO: get-all
;; TODO: between

(defn filter
  [query pred-fn]
  (reql-ast :FILTER [query pred-fn] nil))

;; Document manipulation

(defn get-field
  [query field]
  (reql-ast :GET_FIELD [query field] nil))

;; common alias, used in the other drivers as shorthand
(def g get-field)

;; String manipulation

(defn match
  "Match a string against a regular expression. If there is a match, returns an
  object with the fields:

    str: The matched string
    start: The matched string’s start
    end: The matched string’s end
    groups: The capture groups defined with parentheses

  If no match is found, returns null.

  Accepts RE2 syntax. You can enable case-insensitive matching by prefixing the
  regular expression with (?i). See the RE2 documentation for more flags.

  The match command does not support backreferences."
  [string regexp]
  (reql-ast :MATCH [string regexp] nil))

(defn split
  "Split a string into substrings. With no arguments, will split on whitespace;
  when called with a string as the first argument, will split using that string
  as a separator. A maximum number of splits can also be specified. (To specify
  max_splits while still splitting on whitespace, use null as the separator
  argument.)

  Mimics the behavior of Python’s string.split in edge cases, except for
  splitting on the empty string, which instead produces an array of
  single-character strings."
  ([string]
   (reql-ast :SPLIT [string] nil))
  ([string seperator]
   (reql-ast :SPLIT [string seperator] nil))
  ([string seperator max_splits]
   (reql-ast :SPLIT [string seperator max_splits] nil)))

(defn upcase
  "Uppercases a string."
  [string]
  (reql-ast :UPCASE [string] nil))

(defn downcase
  "Lowercase a string."
  [string]
  (reql-ast :DOWNCASE [string] nil))

;; Math and logic

(defn add
  "Sum two or more numbers, or concatenate two or more strings or arrays.

  The add command can be called in either prefix or infix form; both forms are
  equivalent. Note that ReQL will not perform type coercion. You cannot, for
  example, add a string and a number together."
  [value1 value2 & values]
  (reql-ast :ADD (into [value1 value2] values) nil))

(defn sub
  "Subtract two numbers."
  [value & values]
  (reql-ast :SUB (into [value] values) nil))

(defn mul
  "Multiply two numbers, or make a periodic array."
  [value & values]
  (reql-ast :MUL (into [value] values) nil))

(defn div
  "Divide two numbers."
  [value & values]
  (reql-ast :DIV (into [value] values) nil))

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
  (reql-ast :EQ (into [value1 value2] values) nil))

(defn ne
  "Test if two or more values are not equal."
  [value & values]
  (reql-ast :NE (into [value] values) nil))

(defn gt
  "Compare values, testing if the left-hand value is greater than the right-hand."
  [value & values]
  (reql-ast :GT (into [value] values) nil))

(defn ge
  "Compare values, testing if the left-hand value is greater than or equal to
  the right-hand."
  [value & values]
  (reql-ast :GE (into [value] values) nil))

(defn lt
  "Compare values, testing if the left-hand value is less than the right-hand."
  [value & values]
  (reql-ast :LT (into [value] values) nil))

(defn le
  "Compare values, testing if the left-hand value is less than or equal to the
  right-hand."
  [value & values]
  (reql-ast :LE (into [value] values) nil))

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
  (reql-ast :FUNCALL (into [f] args) nil))

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
