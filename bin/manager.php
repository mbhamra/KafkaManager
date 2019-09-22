#!/usr/bin/env php
<?php

if(!class_exists("KafkaManager\Bridge\KafkaPeclManager")){
    require __DIR__."/../src/KafkaManager/Bridge/KafkaPeclManager.php";
}

declare(ticks = 1);

$km = new KafkaManager\Bridge\KafkaPeclManager();
