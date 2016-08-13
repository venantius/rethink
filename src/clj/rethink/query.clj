(ns rethink.query
  (:refer-clojure :exclude [filter fn update])
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

;; Math and logic

(defn eq
  [value1 value2 & values])

;; Control structures


;; Special
(defn func
  [args terms]
  (reql-ast :FUNC [args] nil))

(defmacro fn [args & [body]]
  (let [new-args (into [] (clojure.core/map
                           #(hash-map :temp-var (keyword %)) args))
        new-replacements (zipmap args new-args)
        new-terms (walk/postwalk-replace new-replacements body)]
    (func new-args new-terms)))

#_(def sample-db (create-class :DB (com.rethinkdb.model.Arguments. "test")))
#_(def db-ast (map->ReqlAst {:term-type :DB
                             :args "test"
                             :optargs nil}))
#_(def b (create-class :TABLE_CREATE (com.rethinkdb.model.Arguments. "test")))

(.value com.rethinkdb.gen.proto.TermType/TABLE_CREATE)
