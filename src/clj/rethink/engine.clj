(ns rethink.engine)

(defn transform-response
  "Given the response of a RethinkDB query, transform it into the appropriate
  Clojure data structure.

  Native Java data structures such as instances of java.util.ArrayList and
  java.util.HashMap will be cast to Clojure vectors and maps.

  Cursors will be cast to sequences using `iterator-seq`, and the individual
  elements in the sequence will be appropriately transformed from their native
  Java data types into Clojure types as above (lazily)"
  [x]
  (cond
    (instance? java.util.ArrayList x)
    (into [] (map transform-response x))

    (instance? java.util.HashMap x)
    (into {} (map transform-response x))

    (instance? java.util.Map$Entry x)
    [(keyword (.getKey x)) (transform-response (.getValue x))]

    ;; TODO: This is fine for now, but we lose the ability to close the cursor
    ;; prematurely if we want to.
    (instance? com.rethinkdb.net.Cursor x)
    (map transform-response (iterator-seq x))
    :else x))

(defn transform-request
  [x]
  (cond
    (map? x)
    (let [f (fn [hm [k v]] (.with hm (name k) (transform-request v)))]
      (reduce f (com.rethinkdb.model.MapObject.) x))

    (vector? x)
    (java.util.ArrayList. (map transform-request x))

    :else x))

(def lookup-table
  {:DB com.rethinkdb.gen.ast.Db
   :DB_CREATE com.rethinkdb.gen.ast.DbCreate
   :DB_DROP com.rethinkdb.gen.ast.DbDrop
   :DB_LIST com.rethinkdb.gen.ast.DbList
   :FILTER com.rethinkdb.gen.ast.Filter
   :INSERT com.rethinkdb.gen.ast.Insert
   :TABLE com.rethinkdb.gen.ast.Table
   :TABLE_CREATE com.rethinkdb.gen.ast.TableCreate
   :TABLE_DROP com.rethinkdb.gen.ast.TableDrop
   :TABLE_LIST com.rethinkdb.gen.ast.TableList})

(defn coerce-to-arguments
  [args]
  (cond (empty? args) (com.rethinkdb.model.Arguments.)
        :else (com.rethinkdb.model.Arguments. args)))

(defn create-class
  "Create a general class using class dispatch on the lookup table. For instance,
  see example b below."
  [term-type args optargs]
  (let [klass (term-type lookup-table)]
    (when-not klass
      (throw (ex-info "Class not found in lookup-table" {})))
    (let [args (coerce-to-arguments args)]
      (clojure.lang.Reflector/invokeConstructor
       klass
       (into-array java.lang.Object [args optargs])))))

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
    (transform-response (.run q conn))))
