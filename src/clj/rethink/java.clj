(ns rethink.java
  (:import [com.rethinkdb RethinkDB]
           [com.rethinkdb.gen.exc.ReqlError]
           [com.rethinkdb.gen.exc.ReqlQueryLogicError]
           [com.rethinkdb.model.MapObject]
           [com.rethinkdb.net.Connection]))

(def r (. RethinkDB r))

;; TODO: Should this be a record?
(defn connect
  [{:keys [hostname port db username password timeout cert-file ssl-context]
    :or {hostname "localhost"
         port 28015
         username "admin"
         password ""
         timeout 20}
    :as opts}]
  (let [connection (.connection r)]
    (when hostname
      (.hostname connection hostname))
    (when port
      (.port connection port))
    (when db
      (.db connection db))
    (when username
      (.user connection username password))
    (when timeout
      (.timeout connection timeout))
    (when cert-file
      (.certFile connection cert-file))
    (when ssl-context
      (.sslContext connection ssl-context))
    (.connect connection)))
