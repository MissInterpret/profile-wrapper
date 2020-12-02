(ns profile-wrapper.core
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
;            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.string :refer [starts-with? includes? split trim]]

            [clojure.math.numeric-tower :as math]
            [taoensso.tufte :as tufte]
;            [sigmund.core :as sig]
            [clj-time.core :as t]
            [clj-time.coerce :as c])
  (:import java.util.Date
           java.lang.management.MemoryUsage
           java.lang.Runtime))

;; Collection ---------------------------------------------
;;
;;  All fns that generate profiling data via the profile
;;  macro.
;;

(defonce store-atm (atom {}))

  ;; Utils ----------------------------------------
  ;;

(defn log [context key]
  #_(log/info key (:event context)))

(defn nano-to-milli [nano]
  (math/round (/ (float nano) 1000000)))

(defn duration-millis [start-inst end-inst]
  (let [s (c/from-date start-inst)
        e (c/from-date end-inst)]
    (math/round (t/in-millis (t/interval s e)))))

(defn bytes-to-mb [bytes]
  (float (/ bytes 1000000)))

(defn interval [{:keys [start duration] :as m}]
  (let [dur (if (nil? duration)
              ; This could be configurable but it determines the interval that a profiled called
              ; will have. It *can* end up missing when data is written during a call.
              ; This definitely happens as the system starts to fall down.
              ; Set a long interval for starters
              (do
                (println "interval> missing duration")
                60000)  ; 60 seconds
              duration)]
    (let [s (c/from-date start)]
      (t/interval s (t/plus s (t/millis dur))))))

(defn during? [{:keys [start]} interval]
  "Did this measurement occur during the profiled fn's
   execution ?"
  (let [s (c/from-date start)]
    (t/within? interval s)))

  ;; Profiles and measurements ----------------------------------------
  ;;

(defn tufte-arg [stats key col]
  (let [v (get-in stats [:stats key col])]
    (nano-to-milli v)))

(defn tufte-max [stats key]
  (tufte-arg @stats key :max))

(defn system-measurement []
  (let [runtime (Runtime/getRuntime)
        cur-free (-> runtime (.freeMemory))
        total-alloc (-> runtime (.totalMemory))
        used (- total-alloc cur-free)
        max (-> runtime (.maxMemory))
       ;; jvm (sig/jvm-memory)
       ;; heap (:heap-memory-usage jvm)
        realized {:committed total-alloc ;; (.getCommitted heap)
                  :max max ;; (.getMax heap)
                  :used used ;; (.getUsed heap)
                  :init 0.0 ;; (.getInit heap)
                  }]
    ;; All the OS info and commented out because of SIGAR dependency that's
    ;; no longer supported
    ;;
    {:jvm realized
     :os {:cpu 0.0 ;;(sig/cpu-usage)
          :mem 0.0 ;; (sig/os-memory)
          :load {:average  0.0 ;; (sig/cpu-usage :average)
                 :load-average  0.0;; (sig/os-load-avg)
                 }}}
    ))

(defn update-stats! [store-atm key stats]
  (let [prev (get @store-atm key)]
    (if (nil? prev)
      (swap! store-atm assoc key stats)
      (swap! store-atm update key tufte/merge-pstats stats))))

(defn measure! [store-atm key]
  "Takes a measurement of the current system stats and adds
   result eo the other measurements that share this tag."
  (let [[result stats] (tufte/profiled {} (tufte/p :measurement (system-measurement)))
        m (-> {:start (Date.)}
              (assoc :data result)
              (assoc :duration (tufte-max stats :measurement)))]
    (swap! store-atm update-in [key :measurements] conj m)
    stats))

(defn new-profile! [store-atm key]
  (let [profile {:start (Date.)
                 :measurements []}]
    (swap! store-atm assoc key profile)))

(defn close-profile! [store-atm caller-key measurement-key stats]
  (let [duration (tufte-max stats caller-key)]
    (swap! store-atm assoc-in [measurement-key :duration] duration)))


(defn profile-key [store-atm caller-key]
  "Converts the caller-key into a measurement key. If the
   base-name already exists then the new tag has an increment. The first tag starts
   with 0.  i.e. :name-0"
  (let [key-name (name caller-key)
        ns-name (namespace caller-key)
        last-key (last (filter (fn [k]
                                 (let [n (name k)
                                       ns-n (namespace k)]
                                   (and
                                      (not (= caller-key k))
                                      (= ns-name ns-n)
                                      (starts-with? n key-name))))
                               (keys @store-atm)))]
    (if (nil? last-key)
      (if (nil? ns-name)
        (keyword (str key-name "-" 0))
        (keyword ns-name (str key-name "-" 0)))
      (let [idx (Integer/parseInt (trim (last (split (name last-key) #"-"))))]
        (if (nil? ns-name)
          (keyword (str key-name "-" (inc idx)))
          (keyword ns-name (str key-name "-" (inc idx))))))))

(defn measurement-fn [store-atm caller-key]
  (let [measurement-key (profile-key store-atm caller-key)]
    (partial measure! store-atm measurement-key)))

(defmacro profile
  [store-atm caller-key & body]
  `(let [measurement-key# (profile-key ~store-atm ~caller-key)
         take-measurement!# (partial measure! ~store-atm measurement-key#)
         measure-fn# (fn []
                        (new-profile! ~store-atm measurement-key#)
                        (take-measurement!#)
                        (let [r# (do ~@body)]
                          (take-measurement!#)
                          r#))]
     (let [[result# stats#] (tufte/profiled {} (tufte/p ~caller-key (measure-fn#)))]
       (update-stats! ~store-atm ~caller-key stats#)
       (close-profile! ~store-atm ~caller-key measurement-key# stats#)
       result#)))


;; Import/Export ------------------------------------------------
;;
;;  Functions that allow export/import cabailities
;;

(defn export-data [store-atm]
  "Transforms in-memory data structures in realized
   data that can be exported/etc."
  (let [raw @store-atm]
    (reduce
      (fn [coll key]
        (let [v (get raw key)]
          (if (map? v)
            (assoc coll key v)
            ; It's several keys deep
            ; the key -> PSTATS -> [:stats key]
            (let [new-v (get-in @v [:stats key])]
              (assoc coll key new-v)))))
      {}
      (keys raw))))

(defn write-data
  ([path]
   (write-data store-atm path))
  ([store-atm path]
   (println "Writing profiling data to " path)
   (let [exported (export-data store-atm)]
     (with-open [w (io/writer path :append false)]
      (pprint exported w)
      exported))))

(defn read-data [path]
  (read-string (slurp path)))


;; Analysis ------------------------------------------------------------
;;
;; NOTE: These functions are to be used ONLY with exported data NOT
;;       raw profiling data in an atom.
;;

  ;; Keys -----------------------------------
  ;;

(defn subkey? [profile-key key]
  "Is this key a sub-key of the profile key?"
  (and
    (not (= profile-key key))
    (= (namespace profile-key) (namespace key))
    (starts-with? (name key) (name profile-key))))

(defn sub-keys [data profile-key]
  (filter
    #(subkey? profile-key %)
    (keys data)))

(defn measurement-key? [key]
  "Does the name end with a -N"
  (try
    (let [n (split (name key) #"-")]
      (int? (Integer/parseInt (last n))))
    (catch Exception e false)))

(defn measurement-keys [data]
  (filter
    #(measurement-key? %)
    (keys data)))

(defn profile-keys [data]
  (filter
    #(not (measurement-key? %))
    (keys data)))

(defn included-profile-keys [data includes-ns]
  "Returns a set of profile keys that include
   the given string as part of its namespace"
  (into #{} (filter
              #(includes? (namespace %) includes-ns)
              (profile-keys data))))


(defn overlapping-keys [data measurement-key]
  "Finds all measurement keys that start during the
   time interval."
  ;(println "overlapping-keys> " measurement-key)
  (let [measurement (get data measurement-key)
        ;_ (println "overlapping-keys> measurement: ")
        ;_ (pprint measurement)
        interval (interval measurement)]
    ;(println "overlapping-keys> interval: " interval)
    (filter
      #(and
        (not= measurement-key %)
        (during? (get data %) interval))
      (measurement-keys data))))


    ;; Data Aggregation ------------------------------------------
    ;;

(defn merge-measurements [data measurement measurement-key]
  (let [result {:count 0
                :merged measurement}
        interval (interval measurement)]
    (reduce
      (fn [r m]
        (if (during? m interval)
          (let [tagged (assoc m :source measurement-key)]
            (-> r
                (update-in [:merged :measurements] conj tagged)
                (update :count (fnil inc 1))))
          r))
      result
      (get-in data [measurement-key :measurements]))))

(defn collect-measurements [data profile-data sub-key]
  ;(println "collect-measurements> " sub-key)
  (let [overlapping-keys (overlapping-keys data sub-key)
        ;_ (println "collect-measurements> overlapping keys:")
        ;_ (pprint overlapping-keys)
        measurement (get data sub-key)]
    ;(println "collect-measurements> measurement:")
    ;(pprint measurement)
    (reduce
      (fn [pd ok]
        (let [{:keys [merged count]} (merge-measurements data measurement ok)]
          (if (> count 0)
            (-> pd
                (update :data conj merged)
                (assoc-in [:from ok] count))
            pd)))
      profile-data
      overlapping-keys)))

(defn stats-millis [data profile-key]
  ;(println "stats-millis> " profile-keys)
  (let [stats (get data profile-key)]
    ;(println "stats-millis> data:")
    ;(pprint stats)
    (-> stats
        (assoc :min (nano-to-milli (:min stats)))
        (assoc :mean (nano-to-milli (:mean stats)))
        (assoc :mad-sum (nano-to-milli (:mad-sum stats)))
        (assoc :p99 (nano-to-milli (:p99 stats)))
        (assoc :p90 (nano-to-milli (:p90 stats)))
        (assoc :max (nano-to-milli (:max stats)))
        (assoc :mad (nano-to-milli (:mad stats)))
        (assoc :p50 (nano-to-milli (:p50 stats)))
        (assoc :sum (nano-to-milli (:sum stats)))
        (assoc :p95 (nano-to-milli (:p95 stats))))))

(defn profile-data [data profile-key]
  ; stats
  ; data []  (partially ordered by start time)
  ; from {}  :measurement-key
  ;(println "profile-data> profile key=" profile-key)
  (let [sub-keys (sub-keys data profile-key)
        ;_ (println "profile-data> sub-keys=" sub-keys)
        collected-data (reduce
                         (partial collect-measurements data)
                         {:data [] :from {}}
                         sub-keys)]
    (assoc collected-data :stats (stats-millis data profile-key))))


    ;; Reporting -----------------------------------------
    ;;

(defn measurement-attribute [measurement path]
  (let [measurements (:measurements measurement)]
    (reduce
      (fn [r sysm]
        ; If the parent of the path is an index
        ; enforce that the parent it indexes
        ; into is an indexable seq
        ;
        ; Quick and dirty it only handles numbers in the
        ; last two positions.
        ;
        (let [value (cond
                      ; [:a :b N :c]
                      (number? (first (take-last 2 path))) (do
                                                             (let [parent-path (drop-last 2 path)
                                                                   parent (into [] (get-in sysm parent-path))]
                                                               (get-in parent (take-last 2 path))))
                      ; [:a :b N]
                      (number? (last path)) (do
                                              (let [parent-path (drop-last path)
                                                    parent (into [] (get-in sysm parent-path))]
                                                (nth parent (last path))))

                      :else (get-in sysm path))]
          (if (or (nil? value) (not (number? value)) (Double/isNaN value))
            (do
              (println "measurement-attribute> (get) INVALID VALUE: " path " value=" value)
              r)
            (conj r value))))
      []
      (get measurement :measurements))))

(defn measurement-min-max-stats [profile-data path]
  "Returns the min, max, and average duration in millis that
   have elapsed due to measurements during the profiling event"
  (reduce
    (fn [r m]
      (let [times (if (= -1 (:min r))
                    (conj (measurement-attribute m path))
                    (conj (measurement-attribute m path) (:min r) (:max r)))
            min (apply min times)
            max (apply max times)]
        {:min min :max max}))
    {:min -1 :max 0}
    (get profile-data :data)))

(defn measurement-times [profile-data]
  (measurement-min-max-stats profile-data [:duration]))

(defn memory-stats [profile-data]
  (let [jvm (measurement-min-max-stats profile-data [:data :jvm :used])
        jvm-used (- (:max jvm) (:min jvm))
        os (measurement-min-max-stats profile-data [:data :os :mem :actual-used])
        os-used (- (:max os) (:min os))
        total (+ jvm-used os-used)]
    ; Convert to MB
    {:min {:jvm (bytes-to-mb (:min jvm)) :os (bytes-to-mb (:min os))}
     :max {:jvm (bytes-to-mb (:max jvm)) :os (bytes-to-mb (:max os))}
     :used {:jvm (bytes-to-mb jvm-used) :os (bytes-to-mb os-used) :total (bytes-to-mb total)}}))

(defn data-for-keys [profile-data-fn data profile-keys]
  (reduce
    (fn [coll k]
      (let [pd (profile-data data k)]
        (assoc coll k (profile-data-fn pd))))
    {}
    profile-keys))

(defn num-cores []
  (let [sys (system-measurement)]
    (count (get-in sys [:os :cpu]))))

(defn core-used [profile-data core-index]
  (let [mm (measurement-min-max-stats profile-data [:data :os :cpu core-index :user])]
    (- (:max mm) (:min mm))))

(defn core-stats [profile-data]
  (let [cores (num-cores)]
    {:total cores
     :stats (reduce
              (fn [coll idx]
                (let [used (core-used profile-data idx)
                      n (keyword (str "core-" idx))]
                  (assoc coll n used)))
              {}
              (into [] (range 1 cores)))}))

(def core-data (partial data-for-keys core-stats))


(defn load-stats [profile-data]
  (let [user (measurement-min-max-stats profile-data [:data :os :load :average :user])
        one (measurement-min-max-stats profile-data [:data :os :load :load-average 0])
        five (measurement-min-max-stats profile-data [:data :os :load :load-average 1])]
    {:user user
     :load-average {:one (assoc one :diff (- (:max one) (:min one)))
                    :five (assoc five :diff (- (:max five) (:min five)))}}))

(def load-data (partial data-for-keys load-stats))

  ;; Sorting ---------------------------------------
  ;;

(defn- attribute-cmp [attribute-path data1 data2]
  ; -1 for RHS
  ;  1 for LHS
  ; 0 for equal
  (let [a1 (get-in data1 attribute-path)
        a2 (get-in data2 attribute-path)]
    (cond
      (and (nil? a1) (nil? a2)) 0
      (nil? a1)                 1
      (nil? a2)                 -1
      :else (compare a2 a1))))

(defn- attribute-by-key-cmp [data attribute-path pk1 pk2]
  (let [pd1 (profile-data data pk1)
        pd2 (profile-data data pk2)]
    (attribute-cmp attribute-path pd1 pd2)))

(defn- sort-keys-by [data attribute-path profile-keys]
  (sort (partial attribute-by-key-cmp data attribute-path) profile-keys))

(defn- get-attrs-in [data path keys data-fn]
  (reduce
    (fn [r k]
      (let [pd (data-fn data k)
            v (get-in pd path)]
        (assoc r k v)))
    {}
    keys))

(defn sorted-durations [data profile-keys]
  (let [path [:stats :mean]
        sorted (sort-keys-by data path profile-keys)]
    (get-attrs-in data path sorted profile-data)))

(defn memory-cmp [data attribute-path pk1 pk2]
  (let [pd1 (profile-data data pk1)
        pd2 (profile-data data pk2)
        mem1 (memory-stats pd1)
        mem2 (memory-stats pd2)]
    (attribute-cmp attribute-path mem1 mem2)))

(defn memory-data [data profile-key]
  (memory-stats (profile-data data profile-key)))

(defn sorted-memory [data profile-keys]
  (let [path [:used :total]
        sorted (sort (partial memory-cmp data path) profile-keys)]
    ; Return the entire used data
    (get-attrs-in data [:used] sorted memory-data)))
