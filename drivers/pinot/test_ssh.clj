(ns test (:require [metabase.driver.sql-jdbc.connection.ssh-tunnel :as ssh])) (ssh/with-ssh-tunnel [details-with-tunnel details] (println details-with-tunnel))
