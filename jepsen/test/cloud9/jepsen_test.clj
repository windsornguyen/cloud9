(ns cloud9.jepsen-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [cloud9.jepsen :as cloud9]
            [jepsen.checker :as checker]
            [jepsen.history :as history]))

(def test-config
  {:nodes       ["n1" "n2" "n3"]
   :client-port 19090
   :raft-port   19091})

(deftest generated-node-config-identifies-the-node-and-full-cluster
  (let [config (cloud9/node-config test-config "n2")]
    (is (str/includes? config "id = 1\nhost = \"n2\""))
    (is (str/includes? config (str "raft_key = \"" cloud9/raft-key "\"")))
    (doseq [[id node] (map-indexed vector (:nodes test-config))]
      (is (str/includes? config (str "{ id = " id ", host = \"" node "\""))))))

(deftest value-codec-round-trips-jepsen-values
  (doseq [value [nil 0 4 {:nested [1 2 3]}]]
    (is (= value (cloud9/decode-value (cloud9/encode-value value))))))

(deftest rpc-retry-classification-is-fail-closed
  (testing "leadership and transient transport failures are retryable"
    (is (cloud9/recoverable-rpc-error?
          {:status 400 :body {:code "failed_precondition" :message "not leader"}}))
    (is (cloud9/recoverable-rpc-error? {:status 503})))
  (testing "application failures are final"
    (is (not (cloud9/recoverable-rpc-error?
               {:status 400 :body {:code "invalid_argument" :message "not leader"}})))
    (is (not (cloud9/recoverable-rpc-error? {:status 409})))
    (is (not (cloud9/recoverable-rpc-error? {:status 412})))))

(deftest cas-failures-require-the-exact-connect-code
  (is (cloud9/expected-cas-failure? {:status 400
                                     :body {:code "failed_precondition"}}))
  (is (cloud9/expected-cas-failure? {:status 404
                                     :body {:code "not_found"}}))
  (is (not (cloud9/expected-cas-failure? {:status 400
                                          :body {:code "invalid_argument"}})))
  (is (not (cloud9/expected-cas-failure? {:status 500
                                          :body {:code "failed_precondition"}}))))

(deftest workload-builds-with-and-without-the-nemesis
  (doseq [mode ["none" "kill-leader"]]
    (let [workload (cloud9/kv-workload {:nemesis-mode mode
                                        :nemesis-interval 1
                                        :stagger 0
                                        :time-limit 1})]
      (is (:checker workload))
      (is (:client workload))
      (is (:generator workload)))))

(deftest failure-only-history-is-not-green
  (let [events (history/history [{:process 0 :type :invoke :f :cas}
                                 {:process 0 :type :fail :f :cas}])
        result (checker/check (checker/stats) {} events {})]
    (is (not= true (:valid? result)))))

(deftest unhandled-client-exception-fails-the-checker
  (let [events (history/history [{:process 0 :type :info :f :read
                                  :exception {:via [{:type "boom"}]}}])
        result (checker/check (cloud9/->NoExceptionsChecker) {} events {})]
    (is (false? (:valid? result)))
    (is (= 1 (:count result)))))
