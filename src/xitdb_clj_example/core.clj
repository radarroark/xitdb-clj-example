(ns xitdb-clj-example.core
  (:import [io.github.radarroark.xitdb
            CoreFile Hasher Database
            Database$ContextFunction Database$Bytes
            ReadCursor WriteCursor
            ReadArrayList ReadHashMap
            WriteArrayList WriteHashMap]
           [java.io File RandomAccessFile]))

(defn -main [& args]
  (with-open [raf (RandomAccessFile. (File. "main.db") "rw")]
    (let [core (CoreFile. raf)
          hasher (Hasher. (java.security.MessageDigest/getInstance "SHA-1"))
          db (Database. core hasher)
          history (WriteArrayList. (.rootCursor db))]
      (.appendContext history
                      (.getSlot history -1)
                      (reify Database$ContextFunction
                        (^void run [this ^WriteCursor cursor]
                          (let [moment (WriteHashMap. cursor)]
                            (.put moment "foo" (Database$Bytes. "bar")))))))))
