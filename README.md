# Kafka Manager for PHP
Continue keep running consumers in background in php.

PHP daemon for kafka consumers

## Pre Requisites

- PHP 5.6 (Tested with PHP 7)
- POSIX extension
- Process Control extension
- Install librdkafka (https://github.com/edenhill/librdkafka)
- Install rdkafka (https://arnaud.le-blanc.net/php-rdkafka/phpdoc/rdkafka.setup.html)

## What is Kafka Manager and use ?

We need to keep running consumers for long time. Continue keep running any php file is very big task. We need to create many files with same code for every topic consumer. To reduce same code redundancy we create a manager which manage topic consumers with same code base. So you can focus on your business logic.

By using of this kafka manager, all you need to do is write the code that actually does the work and not all the repetitive worker setup. Just need to create files in specific directory. All files located in specified directory called as worker to consume messages from specific topic. 

## How it works ?

First of all, we need to create worker file(s) in specific directory. For this example, lets say we create a directory called worker_dir to hold all our worker code.

We can do this by two ways:
- by function
- by class

Example by function
```
# worker_dir/example_function.php

    function example_function($job, &$log) {
    
        // payload message consumed from topic
        $payload = $job->payload();
        
        // write your business logic here
    
        // Log is an array that is passed in by reference that can be
        // added to for logging data that is not part of the return data
        $log[] = "Success";
    
        // return your result to kafka manager to write in kafka manager log file
        return $result;

    }
```

Example by class
```
# worker_dir/ExampleFunction.php

    class ExampleFunction {
    
        public function run($job, &$log) {
        
            // payload message consumed from topic
            $payload = $job->payload();
            
            // write your business logic here
        
            // Log is an array that is passed in by reference that can be
            // added to for logging data that is not part of the return data
            $log[] = "Success";
        
            // return your result to kafka manager to write in kafka manager log file
            return $result;
    
        }
    }
```

That's all.
Enjoy your code. :)

## 

Thanks for using kafka manager and help us to make this perfect.