(ns rethink.engine
  (:require [rethink.meta :as m]))

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

(defn reql-ast?
  [x]
  (and
   (map? x)
   (contains? x :term-type)
   (contains? x :args)
   (contains? x :optargs)))

(defn term-to-classname
  "Given a term, e.g. :DB_CREATE, return the corresponding Java driver class
  name as a symbol, e.g. com.rethinkdb.gen.ast.DbCreate"
  [term]
  (symbol (str "com.rethinkdb.gen.ast."
               (clojure.string/join
                (map clojure.string/capitalize
                     (-> term name (clojure.string/split #"_")))))))

(defn- term-to-class
  "Given a term, e.g. DB_CREATE, resolve to the corresponding Java driver class,
  e.g. com.rethinkdb.gen.ast.DbCreate"
  [term]
  (try
    (resolve
     (term-to-classname term))
    (catch Exception e
      nil)))

(defn generate-lookup-table
  []
  (let [term-types (keys m/term-info)]
    (zipmap term-types
            (map term-to-class term-types))))

(def lookup-table
  (assoc
   (generate-lookup-table)
   :FUNC com.rethinkdb.gen.ast.CljFunc))

#_(def lookup-table
    {:ADD com.rethinkdb.gen.ast.Add
     :DATUM com.rethinkdb.gen.ast.Datum
     :DB com.rethinkdb.gen.ast.Db
     :DB_CREATE com.rethinkdb.gen.ast.DbCreate
     :DB_DROP com.rethinkdb.gen.ast.DbDrop
     :DB_LIST com.rethinkdb.gen.ast.DbList
     :DIV com.rethinkdb.gen.ast.Div
     :EQ com.rethinkdb.gen.ast.Eq
     :FILTER com.rethinkdb.gen.ast.Filter
     :FUNC com.rethinkdb.gen.ast.CljFunc ;; Custom class
     :FUNCALL com.rethinkdb.gen.ast.Funcall
     :GET_FIELD com.rethinkdb.gen.ast.GetField
     :MAKE_ARRAY com.rethinkdb.gen.ast.MakeArray
     :MOD com.rethinkdb.gen.ast.Mod
     :MUL com.rethinkdb.gen.ast.Mul
     :INSERT com.rethinkdb.gen.ast.Insert
     :SUB com.rethinkdb.gen.ast.Sub
     :TABLE com.rethinkdb.gen.ast.Table
     :TABLE_CREATE com.rethinkdb.gen.ast.TableCreate
     :TABLE_DROP com.rethinkdb.gen.ast.TableDrop
     :TABLE_LIST com.rethinkdb.gen.ast.TableList
     :VAR com.rethinkdb.gen.ast.Var})

(defn coerce-to-arguments
  [args]
  (cond (empty? args) (com.rethinkdb.model.Arguments.)
        (coll? args) (com.rethinkdb.model.Arguments. (transform-request args))
        :else (com.rethinkdb.model.Arguments. args)))

(defn coerce-to-optargs
  [m]
  (let [f (fn [hm [k v]] (.with hm (name k) (transform-request v)))]
    (reduce f (com.rethinkdb.model.OptArgs.) m)))

(defn create-class
  "Create a general class using class dispatch on the lookup table. For instance,
  see example b below."
  [term-type args optargs]
  (let [klass (get lookup-table term-type)]
    (when-not klass
      (throw (ex-info (format "Class not found in lookup-table: %s" term-type)
                      {:missing-class term-type})))
    (cond
      (= klass com.rethinkdb.gen.ast.Datum)
      (com.rethinkdb.gen.ast.Datum. (coerce-to-arguments args))
      (= klass com.rethinkdb.gen.ast.CljFunc)
      (com.rethinkdb.gen.ast.CljFunc. (coerce-to-arguments args))

      :else
      (let [args (coerce-to-arguments args)]
        (clojure.lang.Reflector/invokeConstructor
         klass
         (object-array [args optargs]))))))

(defn maybe-cast-to-reql-ast
  [{:keys [term-type args optargs] :as arg}]
  (if (reql-ast? arg)
    (create-class term-type (map maybe-cast-to-reql-ast args) (coerce-to-optargs optargs))
    (transform-request arg)))

(defn run
  [reql-ast conn]
  (let [q (maybe-cast-to-reql-ast reql-ast)]
    (transform-response (.run q conn))))
