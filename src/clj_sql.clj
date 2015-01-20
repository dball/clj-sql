(ns clj-sql
  (:import [java.sql Connection]))

(def isolation-levels
  {:none             java.sql.Connection/TRANSACTION_NONE
   :read-committed   java.sql.Connection/TRANSACTION_READ_COMMITTED
   :read-uncommitted java.sql.Connection/TRANSACTION_READ_UNCOMMITTED
   :repeatable-read  java.sql.Connection/TRANSACTION_REPEATABLE_READ
   :serializable     java.sql.Connection/TRANSACTION_SERIALIZABLE})

(defn isolation-level
  [isolation]
  (let [level (get isolation-levels isolation)]
    (when-not level
      (let [msg (str ("Unknown isolation level: " isolation))]
        (throw (IllegalArgumentException. msg))))
    level))

(deftype Transaction [^Connection conn])

(defn transact!
  [^Connection conn tx-fn options]
  (let [old-auto-commit (.getAutoCommit conn)
        old-isolation (.getTransactionIsolation conn)
        old-read-only (.isReadOnly conn)]
    (io!
     (try
       (let [{:keys [isolation read-only]} options]
         (.setAutoCommit conn false)
         (when isolation
           (.setTransactionIsolation conn (isolation-level isolation)))
         (when read-only
           (.setReadOnly conn true)))
       (tx-fn (Transaction. conn))
       (finally
         (.setAutoCommit conn old-auto-commit)
         (.setTransactionIsolation conn old-isolation)
         (.setReadOnly conn old-read-only))))))

(defn abduce
  [xform f init tx query]
  (let [f (xform f)
        conn (.conn tx)]
    (when (.isClosed conn)
      (throw (IllegalStateException. "The transaction's connection is closed")))
    (with-open [st (.createStatement conn)
                rs (.executeQuery st query)]
      (let [metadata (.getMetaData rs)
            columns (range 1 (inc (.getColumnCount metadata)))
            keys (mapv #(keyword (.getColumnLabel metadata %)) columns)
            next-record-fn (fn [] (zipmap keys (map #(.getObject rs %) columns)))]
        (loop [ret init]
          (if (.next rs)
            (let [ret (f ret (next-record-fn))]
              (if (reduced? ret)
                (f @ret)
                (recur ret)))
            (f ret)))))))

(defn fetch
  ([tx query]
     (abduce identity conj [] tx query))
  ([xform tx query]
     (abduce xform conj [] tx query)))

(comment
  (transact! conn #(fetch (take 2) % "SELECT * FROM clients") {})
  (sql '[:mysql/select
         :table "memberships"
         :where (= ?id "id")
         :lock :write]
       {:id 5})
  ["SELECT * FROM memberships WHERE id = ? FOR UPDATE" 5])
