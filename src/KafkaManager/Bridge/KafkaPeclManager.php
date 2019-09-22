<?php

namespace KafkaManager\Bridge;
use \KafkaManager\KafkaManager;
use KafkaManager\Job;

/**
 * author manmohan <mmsingh62@gmail.com>
 *
 * Implements the worker portions of the kafka consumers
 *
 * @package     KafkaManager
 *
 */

if (!class_exists("KafkaManager")) {
    require dirname(__FILE__)."/../KafkaManager.php";
}


/**
 * Implements the worker portions of the library
 */
class KafkaPeclManager extends KafkaManager {

    protected function start_lib_worker($worker_list, $timeouts = []) {
        $this->log('called start_lib_worker of KafkaPeclManager', KafkaManager::LOG_LEVEL_CRAZY);
        $pid = getmypid();
        foreach($worker_list as $worker) {
            $config = $this->getWorkerConfiguration($worker);
            $isValidated = $this->validateWorkerConfiguration($config);
            $this->log('Worker : ' . $worker, KafkaManager::LOG_LEVEL_INFO);
            $topic_string = implode(',',$config['topic']);

            if ($isValidated) {
                $conf = new \RdKafka\Conf();
                // Set a rebalance callback to log partition assignments (optional)
                $conf->setRebalanceCb(function (\RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
                    switch ($err) {
                        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                            $this->log('Partition assigned: ' . print_r($partitions,1), KafkaManager::LOG_LEVEL_DEBUG);
                            $kafka->assign($partitions);
                            break;

                        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                            $this->log("Partition revoked: " . print_r($partitions,1), KafkaManager::LOG_LEVEL_DEBUG);
                            $kafka->assign(NULL);
                            break;

                        default:
                            $this->log('Got some error: ' . print_r($err,1), KafkaManager::LOG_LEVEL_INFO);
                            throw new \Exception($err);
                    }
                });
                foreach($config['Config'] as $key => $value) {
                    $this->log('set ' . $key . ' => ' . $value, KafkaManager::LOG_LEVEL_DEBUG);
                    $conf->set($key, $value);
                }
                // $conf->set('group.id', $config['group_id']);
                //set client id for consumer
                $conf->set('client.id', $worker);
/*
                // Initial list of Kafka brokers
                $conf->set('metadata.broker.list', $config['broker_list']);
                // this will wait before retry to failed requests to given topic partition
                $conf->set('retry.backoff.ms', $config['retry_backoff_ms']);
                if (isset($config['message_timeout_ms'])) {
                    $conf->set('message.timeout.ms',$config['message_timeout_ms']);
                }

                if (isset($config['max_in_flight_requests_per_connection'])) {
                    $conf->set('max.in.flight.requests.per.connection',$config['max_in_flight_requests_per_connection']);
                }

                if (isset($config['enable_auto_commit'])) {
                    $conf->set('enable.auto.commit',$config['enable_auto_commit']);
                }

                if (isset($config['retries'])) {
                    $conf->set('retries', $config['retries']);
                }

                // check stats are enabled or not
                //set consumer statistics interval ms
                $conf->set('statistics.interval.ms', $config['statistics_interval_ms']);
                $conf->setStatsCb([$this, 'consumerStats']);

                // set error callback for consumer
                $conf->setErrorCb([$this, 'consumerError']);
*/
                $topicConf = new \RdKafka\TopicConf();

                /*

                // Set where to start consuming messages when there is no initial offset in
                // offset store or the desired offset is out of range.
                $topicConf->set('auto.offset.reset', $config['auto_offset_reset']);
                */
                // set topic level configuration
                foreach($config['TopicConfig'] as $key => $val) {
                    $this->log('set ' . $key . ' => ' . $value, KafkaManager::LOG_LEVEL_DEBUG);
                    $topicConf->set($key, $value);
                }

                // Set the configuration to use for subscribed/assigned topics
                $conf->setDefaultTopicConf($topicConf);

                $consumer = new \RdKafka\KafkaConsumer($conf);

                $this->log('Going to subscribe for topic: ' . $topic_string, KafkaManager::LOG_LEVEL_INFO);
                $consumer->subscribe($config['topic']);

                $this->consumers[$pid]['consumer'] = &$consumer;

                //$this->log('Consumjers pid: ' . $pid. ' : ' . print_r($this->consumers[$pid],1));
                while (!$this->stop_work) {

                    $message = $consumer->consume($config['ConsumerConfig']['timeout']); // timeout
                    switch ($message->err) {
                        case RD_KAFKA_RESP_ERR_NO_ERROR:
                            // logics to give this message to consumer
                            $consumer->commit($message);
                            $job = new Job($worker, $message, $config['custom']);
                            $consumerLogDetails = $this->do_job($job);
                            $this->log('Job done for worker: ' . $worker, KafkaManager::LOG_LEVEL_INFO);
                            break;
                        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                            $this->log('No more message for worker: ' . $worker, KafkaManager::LOG_LEVEL_DEBUG);
                            break;
                        case RD_KAFKA_RESP_ERR__TIMED_OUT:
                            $this->log('Timed out for ' . $worker);
                            break;

                        case RD_KAFKA_RESP_ERR__TRANSPORT:
                            $this->log('Broker transport failure ' . $worker);
                            break;
                        case RD_KAFKA_RESP_ERR__TIMED_OUT:
                            $this->log('Operation timed out ' . $worker);
                            break;
                        default:
                            $this->stop_work = true;
                            throw new \Exception($message->errstr(), $message->err);
                            break;
                    }
                }

            } else {
                throw new \Exception('Configuration not found for worker: ' . $worker);
            }
        }

    }

    /**
     * Wrapper function handler for all registered functions
     * This allows us to do some nice logging when jobs are started/finished
     */
    public function do_job($job) {

        static $objects;

        if ($objects===null) $objects = array();

        $job_name = $job->functionName();

        if ($this->prefix) {
            $func = $this->prefix.$job_name;
        } else {
            $func = $job_name;
        }

        if (empty($objects[$job_name]) && !function_exists($func) && !class_exists($func, false)) {

            if (!isset($this->functions[$job_name])) {
                $this->log("Class $func not found");
                return;
            }

            require_once $this->functions[$job_name]["path"];

            if (class_exists($func) && method_exists($func, "run")) {

                $this->log("Creating a $func object", KafkaManager::LOG_LEVEL_WORKER_INFO);
                $ns_func = "\\$func";
                $objects[$job_name] = new $ns_func();

            } elseif (!function_exists($func)) {

                $this->log("Function $func not found");
                return;
            }

        }
        $this->log("Starting Job: $job_name", KafkaManager::LOG_LEVEL_WORKER_INFO);
        $log = array();

        /**
         * Run the real function here
         */
        if (isset($objects[$job_name])) {
            $this->log("Calling object for $job_name.", KafkaManager::LOG_LEVEL_DEBUG);
            $result = $objects[$job_name]->run($job, $log);
        } elseif (function_exists($func)) {
            $this->log("Calling function for $job_name.", KafkaManager::LOG_LEVEL_DEBUG);
            $result = $func($job, $log);
        } else {
            $this->log("FAILED to find a function or class for $job_name.", KafkaManager::LOG_LEVEL_INFO);
        }

        if (!empty($log)) {
            foreach ($log as $l) {

                if (!is_scalar($l)) {
                    $l = explode("\n", trim(print_r($l, true)));
                } elseif (strlen($l) > 256) {
                    $l = substr($l, 0, 256)."...(truncated)";
                }

                if (is_array($l)) {
                    foreach ($l as $ln) {
                        $this->log("$ln", KafkaManager::LOG_LEVEL_WORKER_INFO);
                    }
                } else {
                    $this->log("$l", KafkaManager::LOG_LEVEL_WORKER_INFO);
                }

            }
        }

        $result_log = $result;

        if (!is_scalar($result_log)) {
            $result_log = explode("\n", trim(print_r($result_log, true)));
        } elseif (strlen($result_log) > 256) {
            $result_log = substr($result_log, 0, 256)."...(truncated)";
        }

        if (is_array($result_log)) {
            foreach ($result_log as $ln) {
                $this->log("$ln", KafkaManager::LOG_LEVEL_DEBUG);
            }
        } else {
            $this->log("$result_log", KafkaManager::LOG_LEVEL_DEBUG);
        }

        $type = gettype($result);
        settype($result, $type);


        $this->job_execution_count++;

        return $result;

    }

    /**
     * Consumer Error function
     *
     * this function is callback function by kafka consumer
     * @param $kafka
     * @param $err
     * @param $reason
     *
     * @throws \Exception
     */
    public function consumerError($kafka, $err, $reason)
    {
        $this->stop_work = true; // stop consumer as it got an error
        $error = sprintf("Kafka error: %s reason: %s", rd_kafka_err2str($err), $reason);
        $this->log($error, KafkaManager::LOG_LEVEL_INFO);
        #throw new \Exception($error);
    }

    /**
     * This function used to captured stats and send it to the graphite host
     * @param $kafka
     * @param $json
     * @param $json_len
     */
    public function consumerStats($kafka, $json, $json_len)
    {
        $statisticsDetails = json_decode($json, true);
        
    }
    
    /**
     * Validates the compatible worker files/functions
     */
    protected function validate_lib_workers() {

        foreach ($this->functions as $func => $props) {
            require_once $props["path"];
            $real_func = $this->prefix.$func;
            if (!function_exists($real_func) &&
                (!class_exists($real_func) || !method_exists($real_func, "run"))) {
                $this->log("Function $real_func not found in ".$props["path"]);
                posix_kill($this->pid, SIGUSR2);
                exit();
            }
        }

    }

}
