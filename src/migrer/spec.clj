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

(s/def :migrations/filename ::migration-file)
(s/def :migrations/type #{:versioned :seed :repeatable})
(s/def :migrations/sql (s/and string? (comp not empty?)))
(s/def :migrations/description (s/and string? (comp not empty?)))
(s/def :migrations/sequence# (s/and string? (comp not empty?)))

(s/def ::migration-map
  (s/keys :req [:migrations/filename
                :migrations/type
                :migrations/sql
                :migrations/description
                :migrations/sequence#]))

(s/def :migrer/reset? boolean?)

(s/def ::options (s/keys :opt [:migrer/root
                               :migrer/table-name
                               :migrer/log-fn
                               :migrer/use-classpath?]))

(s/fdef m/init!
  :args (s/cat :conn ::jdbc/db-spec)
  :ret nil?)

(s/fdef m/migrate
  :args (s/alt :conn (s/cat :conn ::jdbc/db-spec)
               :conn+path (s/cat :conn ::jdbc/db-spec :root ::migration-root)
               :conn+path+opts (s/cat :conn ::jdbc/db-spec :path ::path :opts ::options))
  :ret (s/coll-of ::migration-map))
