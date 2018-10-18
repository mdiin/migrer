(ns migrer.spec
  (:require
   [migrer.core :as m]
   [clojure.spec.alpha :as s]
   [clojure.java.jdbc.spec :as jdbc]))

(s/def ::migration-root (s/and string? #(re-matches #"^.*\/$" %)))
(s/def ::versioned-migration (s/and string? #(re-matches #"^V\d+__.+\.sql$")))
(s/def ::seed-migration (s/and string? #(re-matches #"^S\d+__.+\.sql")))
(s/def ::repeatable-migration (s/and string? #(re-matches #"^R\d+__.+\.sql")))

(s/def ::migration-file (s/or :versioned ::versioned-migration
                              :seed ::seed-migration
                              :repeatable ::repeatable-migration))

(s/def ::migration-path (s/cat :root ::migration-root :file ::migration-file))

(s/def :migrer/reset? boolean?)

(s/def ::options (s/keys :opt [:migrer/reset?]))

(s/fdef m/migrate
  :args (s/alt :conn (s/cat :conn ::jdbc/db-spec)
               :conn+path (s/cat :conn ::jdbc/db-spec :root ::migration-root)
               :conn+path+opts (s/cat :conn ::jdbc/db-spec :path ::path :opts ::options))
  :ret (s/coll-of ::migration-path))
