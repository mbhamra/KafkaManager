<?php

function reverse_string($job, &$log) {

    $payload = $job->payload();

    $result = strrev($payload);

    $log[] = "Success";

    return $result;

}

?>