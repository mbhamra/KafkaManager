<?php

class Sum {

    public function run($job, &$log) {

        $payload = $job->payload();

            $dat = json_decode($payload, true);

            $sum = 0;

            foreach($dat as $d){
                $sum+=$d;
                sleep(1);
            }

        $log[] = "Answer: ".$sum;

        return $sum;

    }

}

?>