(ns rethink.ast.reql-ast
  (:require [rethink.java :as java]))

(defonce conn (java/connect {}))

(defn java->clj
  [x]
  (cond
    (instance? java.util.ArrayList x) (into [] (map java->clj x))
    (instance? java.util.HashMap x) (into {} (map java->clj x))
    (instance? java.util.Map$Entry x) [(keyword (.getKey x)) (java->clj (.getValue x))]
    :else x))

(def lookup-table
  {:DB com.rethinkdb.gen.ast.Db
   :DB_CREATE com.rethinkdb.gen.ast.DbCreate
   :DB_DROP com.rethinkdb.gen.ast.DbDrop
   :TABLE_CREATE com.rethinkdb.gen.ast.TableCreate
   :TABLE_LIST com.rethinkdb.gen.ast.TableList})

(defn create-class
  "Create a general class using class dispatch on the lookup table. For instance,
  see example b below."
  [term-type args optargs]
  (let [klass (term-type lookup-table)]
    (when-not klass
      (throw (ex-info "Class not found in lookup-table" {})))
    (clojure.lang.Reflector/invokeConstructor
     klass
     (into-array java.lang.Object (concat args optargs)))))

(defn reql-ast
  "Generate a Reql Ast map"
  [term-type args optargs]
  {:term-type term-type
   :args args
   :optargs optargs})

(defn maybe-cast-to-reql-ast
  [{:keys [term-type args optargs] :as arg}]
  (if (map? arg)
    (create-class term-type args optargs)
    arg))

(defn recursively-create-class
  [{:keys [term-type args optargs] :as reql-ast}]
  (if args
    (create-class term-type (map maybe-cast-to-reql-ast args) optargs)
    (create-class term-type args optargs)))

(defn run
  [{:keys [term-type args optargs] :as reql-ast} conn]
  (let [q (recursively-create-class reql-ast)]
    (java->clj (.run q conn))))

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

(defn table-create
  ([table-name]
   (table-create table-name nil))
  ([args optargs]
   (reql-ast :TABLE_CREATE [args] optargs)))

(defmulti table-create-test
  (fn [x & args] (:term-type x)))

(defmethod table-create-test :DB
  [db tablename & optargs]
  (reql-ast :TABLE_CREATE [db tablename] optargs))

(defmethod table-create-test :default
  [tablename & optargs]
  (reql-ast :TABLE_CREATE [tablename] optargs))

(defn index-create
  ([table index-name])
  ([table index-name index-fn]))

(defn db
  [args]
  (reql-ast :DB [args] nil))

(defn table-list
  [db]
  (reql-ast :TABLE_LIST [db] nil))

#_(def sample-db (create-class :DB (com.rethinkdb.model.Arguments. "test")))
#_(def db-ast (map->ReqlAst {:term-type :DB
                             :args "test"
                             :optargs nil}))
#_(def b (create-class :TABLE_CREATE (com.rethinkdb.model.Arguments. "test")))

(.value com.rethinkdb.gen.proto.TermType/TABLE_CREATE)
