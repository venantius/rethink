(ns rethink.java
  (:import [com.rethinkdb RethinkDB]
           [com.rethinkdb.gen.exc.ReqlError]
           [com.rethinkdb.gen.exc.ReqlQueryLogicError]
           [com.rethinkdb.model.MapObject]
           [com.rethinkdb.net.Connection]))

(defn connect
  [{:keys [hostname port dbname username password timeout cert-file ssl-context] :as opts}]
  (let [connection (.connection r)]
    (when hostname
      (.hostname connection hostname))
    (when port
      (.port connection port))
    (when dbname
      (.dbname connection dbname))
    (when username
      (.user connection username password))
    (when timeout
      (.timeout connection timeout))
    (when cert-file
      (.certFile connection cert-file))
    (when ssl-context
      (.sslContext connection ssl-context))
    (.connect connection)))
