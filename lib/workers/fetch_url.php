<?php

function fetch_url($job, &$log) {

    $payload = $job->payload();

    $result = file_get_contents($payload);

    $log[] = "Success";

    return $result;

}

?>