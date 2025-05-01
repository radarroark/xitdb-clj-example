(ns xitdb-clj-example.core
  (:require [clojure.test :refer [is]])
  (:import [io.github.radarroark.xitdb
            CoreFile CoreMemory Hasher RandomAccessMemory Tag
            Database Database$ContextFunction Database$Bytes Database$Uint
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
      ;; create new transaction to write data
      (.appendContext history
                      (.getSlot history -1)
                      (reify Database$ContextFunction
                        (^void run [this ^WriteCursor cursor]
                          ;; this will produce a data structure that looks like this:
                          ;;
                          ;; {"foo" "bar"
                          ;;  "fruits" ["apple" "pear" "grape"]
                          ;;  "people" [{"name" "Alice"
                          ;;             "age" 25}
                          ;;            {"name" "Bob"
                          ;;             "age" 42}]}
                          (let [moment (WriteHashMap. cursor)
                                fruits (WriteArrayList. (.putCursor moment "fruits"))
                                people (WriteArrayList. (.putCursor moment "people"))]
                            (.put moment "foo" (Database$Bytes. "bar"))
                            (doto fruits
                              (.append (Database$Bytes. "apple"))
                              (.append (Database$Bytes. "pear"))
                              (.append (Database$Bytes. "grape")))
                            (doto (WriteHashMap. (.appendCursor people))
                              (.put "name" (Database$Bytes. "Alice"))
                              (.put "age" (Database$Uint. 25)))
                            (doto (WriteHashMap. (.appendCursor people))
                              (.put "name" (Database$Bytes. "Bob"))
                              (.put "age" (Database$Uint. 42)))))))
      ;; read the data from the latest transaction
      (let [moment (ReadHashMap. (.getCursor history -1))
            foo-cursor (.getCursor moment "foo")
            fruits (ReadArrayList. (.getCursor moment "fruits"))
            people (ReadArrayList. (.getCursor moment "people"))]
        (is (= "bar" (String. (.readBytes foo-cursor nil))))
        (is (= "apple" (String. (.readBytes (.getCursor fruits 0) nil))))
        (let [bob (ReadHashMap. (.getCursor people 1))
              bob-name (.getCursor bob "name")
              bob-age (.getCursor bob "age")]
          (is (= "Bob" (String. (.readBytes bob-name nil))))
          (is (= 42 (.readUint bob-age))))
        ;; if you don't know the type of a cursor, you can check it like this
        (is (= Tag/ARRAY_LIST (-> moment (.getCursor "people") .slot .tag)))
        (is (= Tag/HASH_MAP (-> moment (.getCursor "people") ReadArrayList. (.getCursor 1) .slot .tag)))
        ;; byte arrays <= 8 are SHORT_BYTES, so you should check for either type like this
        (is (contains? #{Tag/SHORT_BYTES Tag/BYTES} (-> foo-cursor .slot .tag)))))))

