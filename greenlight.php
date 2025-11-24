<?php
require __DIR__ . '/vendor/autoload.php';
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

function say($s){ echo $s; }
function ask($q){
    if (function_exists('readline')) {
        $r = readline($q);
        if ($r !== false) return trim($r);
    }
    echo $q;
    $r = fgets(STDIN);
    return $r === false ? '' : trim($r);
}
$configPath = __DIR__ . '/deploy_config.ini';
if (!is_readable($configPath)) {
    fwrite(STDERR, "[ERROR] Cannot read $configPath\n");
    exit(1);
}
$cfg = parse_ini_file($configPath);


$decision = strtolower($argv[1] ?? '');
if (!in_array($decision, ['pass','fail'], true)) {
    $decision = strtolower(ask("Mark QA result (pass/fail): "));
    while (!in_array($decision, ['pass','fail'], true)) {
        $decision = strtolower(ask("Please type 'pass' or 'fail': "));
    }
}

$status = ($decision === 'pass') ? 'pass' : 'fail';

$data = [
    'qa_status' => $status
];

try {
    $conn = new AMQPStreamConnection(
        $cfg['rabbitHost'],
        5672,
        $cfg['rabbitUser'],
        $cfg['rabbitPass'],
        $cfg['vhost'],
        false,
        'AMQPLAIN',
        null,
        'en_US',
        120.0,
        600.0,
        null,
        true,
        240
    );

    $ch = $conn->channel();
    $ch->queue_declare('prod_request', false, true, false, false);

    $msg = new AMQPMessage(json_encode($data));
    $ch->basic_publish($msg, '', 'prod_request');

    say("Sent QA {$status} to PROD queue (prod_request)\n");

    $ch->close();
    $conn->close();
} catch (Throwable $e) {
    fwrite(STDERR, "[ERROR] Failed: " . $e->getMessage() . "\n");
    exit(1);
}

