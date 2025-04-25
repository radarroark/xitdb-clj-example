(ns xitdb-clj-example.core
  (:import [io.github.radarroark.xitdb
            CoreFile CoreMemory Hasher Database RandomAccessMemory
            Database$ContextFunction Database$Bytes
            ReadCursor WriteCursor
            ReadArrayList ReadHashMap
            WriteArrayList WriteHashMap]
           [java.io File RandomAccessFile]))

(defn run [{:keys [db-kind]}]
  (with-open [ra (case db-kind
                   :file (RandomAccessFile. (File. "main.db") "rw")
                   :memory (RandomAccessMemory.))]
    (let [core (case db-kind
                 :file (CoreFile. ra)
                 :memory (CoreMemory. ra))
          hasher (Hasher. (java.security.MessageDigest/getInstance "SHA-1"))
          db (Database. core hasher)
          history (WriteArrayList. (.rootCursor db))]
      (.appendContext history
                      (.getSlot history -1)
                      (reify Database$ContextFunction
                        (^void run [this ^WriteCursor cursor]
                          (let [moment (WriteHashMap. cursor)]
                            (.put moment "foo" (Database$Bytes. "bar")))))))))
