(defproject thi.ng/ldk-couchdb "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [thi.ng/ldk-core "0.1.0-SNAPSHOT"]
                 [com.ashafa/clutch "0.4.0-RC1"]
                 [com.google.guava/guava "15.0"]]
  :profiles {:dev {:dependencies [[speclj "2.8.1"]
                                  [environ "0.4.0"]]}}
  :plugins [[speclj "2.8.1"]]
  :test-paths ["spec"])
