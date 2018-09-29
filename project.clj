(defproject clj-3df "0.1.0"
  :description "3DF client"
  :plugins [[lein-tools-deps "0.4.1"]]
  :middleware [lein-tools-deps.plugin/resolve-dependencies-with-deps-edn]
  :lein-tools-deps/config {:config-files ["deps.edn" :install :user :project]}

  :profiles {:dev {:dependencies [[cider/piggieback "0.3.9"]]
                   :repl-options {:nrepl-middleware [cider.piggieback/wrap-cljs-repl]}}}

  :main clj-3df.core)
