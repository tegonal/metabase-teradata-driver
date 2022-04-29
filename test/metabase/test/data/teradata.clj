(ns metabase.test.data.teradata
  (:require
    [clojure [string :as s]]
    [clojure.java.jdbc :as jdbc]
    [metabase.driver :as driver]
    [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
    [metabase.test.data
     [interface :as tx]
     [sql :as sql.tx]
     [sql-jdbc :as sql-jdbc.tx]]
    [metabase.test.data.sql-jdbc
     [execute :as execute]
     [load-data :as load-data]]
    [metabase.util :as u])
  (:import  java.sql.SQLException))

(sql-jdbc.tx/add-test-extensions! :teradata)

(defmethod sql.tx/field-base-type->sql-type [:teradata :type/BigInteger] [_ _] "BIGINT")
(defmethod sql.tx/field-base-type->sql-type [:teradata :type/Boolean]    [_ _] "BYTEINT")
(defmethod sql.tx/field-base-type->sql-type [:teradata :type/Date]       [_ _] "DATE")
(defmethod sql.tx/field-base-type->sql-type [:teradata :type/DateTime]   [_ _] "TIMESTAMP")
(defmethod sql.tx/field-base-type->sql-type [:teradata :type/Decimal]    [_ _] "DECIMAL")
(defmethod sql.tx/field-base-type->sql-type [:teradata :type/Float]      [_ _] "FLOAT")
(defmethod sql.tx/field-base-type->sql-type [:teradata :type/Integer]    [_ _] "INTEGER")
(defmethod sql.tx/field-base-type->sql-type [:teradata :type/Text]       [_ _] "VARCHAR(2048)")
(defmethod sql.tx/field-base-type->sql-type [:teradata :type/Time]       [_ _] "TIME")

;; Tested using Teradata Express VM image. Set the host to the correct address if localhost does not work.
(def ^:private connection-details
  (delay
    {:host     (tx/db-test-env-var-or-throw :teradata :host "10.42.10.210")
     :user     (tx/db-test-env-var-or-throw :teradata :user "dbc")
     :password (tx/db-test-env-var-or-throw :teradata :password "dbc")}))

(defmethod tx/dbdef->connection-details :teradata [& _] @connection-details)

;
;(defmethod tx/aggregate-column-info :teradata
;  ([driver ag-type]
;    ((get-method tx/aggregate-column-info ::tx/test-extensions) driver ag-type))
;
;  ([driver ag-type field]
;    (merge
;     ((get-method tx/aggregate-column-info ::tx/test-extensions) driver ag-type field)
;     (when (= ag-type :sum)
;       {:base_type :type/Decimal}))))


(defmethod sql.tx/drop-table-if-exists-sql :teradata [_ {:keys [database-name]} {:keys [table-name]}]
  (format "DROP TABLE \"%s\".\"%s\"" database-name table-name))

(defmethod sql.tx/create-db-sql :teradata [_ {:keys [database-name]}]
  (format "CREATE user \"%s\" AS password=\"%s\" perm=524288000 spool=524288000" database-name database-name))

(defmethod sql.tx/drop-db-if-exists-sql :teradata [_ {:keys [database-name]}]
  (format "DELETE user \"%s\" ALL; DROP user \"%s\";" database-name database-name))


(defmethod execute/execute-sql! :teradata  [driver context dbdef sql]
  (println "teradata ---------------------------")
  (println sql)
  (try
    (apply execute/sequentially-execute-sql!  [driver context dbdef sql])
    (catch Throwable e
      (println (.getMessage e))
      ;      ;; TODO Ignoring "drop if exists" errors. Remove as soon as we have a better solution.
      (if (not (or (s/includes? (.getMessage e) "Error 3807")
                   (s/includes? (.getMessage e) "Error 3802")))
        (throw e) (.printStackTrace e)))
    )
  )

;(defmethod execute/execute-sql! :teradata [& args]
;  (println (u/format-color 'blue "[teradata] %s.%s" args))
;  (try
;    (apply execute/sequentially-execute-sql! args)
;    (catch SQLException e
;      ;; TODO Ignoring "drop if exists" errors. Remove as soon as we have a better solution.
;      (if (not (or (s/includes? (.getMessage e) "Error 3807")
;                   (s/includes? (.getMessage e) "Error 3802")))
;        (throw e)))
;    ))


(defmethod load-data/load-data! :teradata [& args]
  (apply load-data/load-data-one-at-a-time! args))

(defmethod sql.tx/pk-sql-type :teradata [_]
  "INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE -2147483647 MAXVALUE 2147483647 NO CYCLE)")

;
;;(defmethod execute/execute-sql! :teradata
;;  [driver _ dbdef sql]
;;  (println sql)
;;  ;; we always want to use 'server' context when execute-sql! is called (never
;;  ;; try connect as GUEST, since we're not giving them priviledges to create
;;  ;; tables / etc)
;;  ((get-method execute/execute-sql! :sql-jdbc/test-extensions) driver :server dbdef sql))
;
;(defmethod execute/execute-sql! :teradata [& args]
;  (println "heeeeeeeeeeeeeeere")
;  (apply execute/sequentially-execute-sql! args))
;
;(defmethod execute/execute-sql! :teradata [& args]
;  (println (u/format-color 'blue "[teradata] %s.%s" args))
;  (try
;    (apply execute/sequentially-execute-sql! args)
;    (catch SQLException e
;      (println (u/format-color 'red "[teradata] %s" (.getMessage e)))
;;      (println "Error executing SQL:" sql)
;;      (printf "Caught SQLException:\n%s\n"
;;              (with-out-str (jdbc/print-sql-exception-chain e)))
;      ;; TODO Ignoring "drop if exists" errors. Remove as soon as we have a better solution.
;      (if (not (or (s/includes? (.getMessage e) "Error 3807")
;                   (s/includes? (.getMessage e) "Error 3802")))
;        (throw e)))
;    (catch Throwable e
;;      (println "Error executing SQL:" sql)
;      (printf "Caught Exception: %s %s\n%s\n" (class e) (.getMessage e)
;              (with-out-str (.printStackTrace e)))
;      (throw e))))
;
;
;
;(defn- dbspec [& _]
;  (sql-jdbc.conn/connection-details->spec :teradata @connection-details))
;;
;;(defn- execute! [format-string & args]
;;  (let [sql (apply format format-string args)]
;;    (println (u/format-color 'blue "[teradata] %s" sql))
;;    (jdbc/execute! (dbspec) sql))
;;  (println (u/format-color 'green "[ok]")))
;;
;;
;;(defn create-user!
;;  ([username]
;;   (create-user! username username))
;;  ([username password]
;;   (execute! "CREATE user \"%s\" AS password=\"%s\" perm=524288000 spool=524288000;"
;;             username
;;             password)))
;;
;;(defn drop-user! [username]
;;  (u/ignore-exceptions
;;   (execute! "DROP USER \"%s\"" username)))
;;
;;(defmethod tx/before-run :teradata
;;  [_]
;;  (drop-user! "sample-dataset")
;;  (create-user! "sample-dataset"))
;
(defmethod sql.tx/qualified-name-components :teradata
  ([_ db-name]                       [db-name])
  ([_ db-name table-name]            [db-name table-name])
  ([_ db-name table-name field-name] [db-name table-name field-name]))

;
;(defmethod sql.tx/create-db-sql :teradata [& _] nil)
;(defmethod sql.tx/drop-db-if-exists-sql :teradata [& _] nil)
;
;;;; Clear out the session schema before and after tests run
;;; TL;DR Teradata schema == Teradata user. Create new user for session-schema
;(defn- execute! [format-string & args]
;  (let [sql (apply format format-string args)]
;    (println (u/format-color 'blue "[teradata] %s" sql))
;    (jdbc/execute! (dbspec) sql))
;  (println (u/format-color 'blue "[ok]")))
;
;(defn create-user!
;  ;; default to using session-password for all users created this session
;  ([username]
;   (create-user! username session-password))
;  ([username password]
;   (execute! "CREATE USER \"%s\" IDENTIFIED BY \"%s\" DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS"
;             username
;             password)))
;
;(defn drop-user! [username]
;  (u/ignore-exceptions
;   (execute! "DROP USER %s CASCADE" username)))
;
;(defmethod tx/before-run :teradata
;  [_]
;  (drop-user! session-schema)
;  (create-user! session-schema))


:aliases {
           :user/teradata-driver {
                                   :extra-paths ["/home/robert/projects/ght/metabase-teradata-driver/test"]
                                   :extra-deps {
                                                 metabase/teradata-driver {:local/root "/home/robert/projects/ght/metabase-teradata-driver"}
                                                 ;        expectations/clojure-test {:mvn/version "1.2.1"}
                                                 expectations/expectations {:mvn/version "2.1.10"}
                                                 teradata/teradata-jdbc {:local/root "/cloud/tegonal_filestore/projects/ongoing/swisscom/metabase_teradata/teradata-jdbc/17.10.00.27/terajdbc4.jar"}
                                                 }
                                   }
           }
