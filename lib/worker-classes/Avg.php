<?php

class Avg {
    
    public function run($job, &$log) {

        $payload = $job->payload();
        $dat = json_decode($payload, true);

        $sum = 0;

        foreach($dat as $d){
            $sum+=$d;
            sleep(1);
        }

        $avg = $sum / count($dat);

        $log[] = "Answer: ".$avg;

        return $avg;

    }

}

?>