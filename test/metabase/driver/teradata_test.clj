;(ns metabase.driver.teradata-test
;  (:require [clojure.test :refer :all]
;            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]))
;
;(deftest connection-details->spec-test
;  (testing "connection config"
;  (let [actual-spec (-> (sql-jdbc.conn/connection-details->spec :teradata {:host "localhost"
;                                                                           :additional-options  "COP=OFF"}))
;        expected-spec {:classname                   "com.teradata.jdbc.TeraDriver"
;                       :subprotocol                 "teradata"
;                       :subname                     "//localhost/CHARSET=UTF8,TMODE=ANSI,ENCRYPTDATA=ON,FINALIZE_AUTO_CLOSE=ON,LOB_SUPPORT=OFF,COP=OFF"
;                       }
;        ]
;  (is (= expected-spec actual-spec)))
;))
;; Check that additional JDBC options are handled correctly. This is comma separated for Teradata.
;(expect
;
;  (-> (sql-jdbc.conn/connection-details->spec :teradata {:host "localhost"
;                                                         :additional-options  "COP=OFF"})))