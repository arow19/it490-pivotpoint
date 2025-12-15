<?php

//testing to see if this changed
require_once __DIR__ . '/vendor/autoload.php';

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

$APIKeyFile = fopen("../doNotPushToGit/finnhubAPIKey", "r") or die("Unable to open file!");
$apiKey = trim(fread($APIKeyFile,filesize("../doNotPushToGit/finnhubAPIKey")));
fclose($APIKeyFile);
echo "Starting sell stock consumer\n";

$config = parse_ini_file('/home/andrew/rabbitmqconfig/rabbitconfig.ini');
$rabbitHost = $config['rabbitHost'];
$rabbitUser = $config['rabbitUser'];
$rabbitPass = $config['rabbitPass'];

$vhost  = $config['vhost'];
$connection = new AMQPStreamConnection($rabbitHost, 5672, $rabitUser, $rabbitPass, $vhost);
$channel = $conn->channel();
$channel->queue_declare('request_sell_stock', false, true, false, false);
$channel->queue_declare('response_sell_stock', false, true, false, false);
    echo "Waiting for sell stock request...\n";

    $callback = function ($msg) use ($channel, $apiKey) {
        echo "Received sell stock request: {$msg->body}\n";

        $data = json_decode($msg->body, true);
        if (!is_array($data) || empty($data['symbol'])) {
            echo "Invalid message format\n";
            return;
        }

        $symbol = strtoupper(trim($data['symbol']));
        $corrId = $msg->get('correlation_id');
        $replyTo = $msg->get('reply_to') ?: 'response_sell_stock';

        $price = null;
        $error = null;
        
        $url = "https://finnhub.io/api/v1/quote?symbol={$symbol}&token={$apiKey}";
        $response = @file_get_contents($url);
            if ($response !== false) {
                $json = json_decode($response, true);
                if (isset($json['c']) && $json['c'] > 0) {
                    $price = floatval($json['c']);
                } else {
                    $error = 'Invalid price data from API';
                }
            } else {
                $error = 'API request failed';
            }
        $payload = [
            'status' => $price ? 'success' : 'error',
            'symbol' => $symbol,
            'price'  => $price,
            'message'=> $price ? "Live price fetched for {$symbol}" : ($error ?? 'Unknown error')
        ];
        $responseMsg = new AMQPMessage(json_encode($payload),
            [
                'correlation_id' => $corrId,
                'content_type'   => 'application/json'
            ]
        );

        $channel->basic_publish($responseMsg, '', $replyTo);
        echo "Published response for {$symbol}: " . json_encode($payload) . "\n";
    };
    $channel->basic_consume('request_sell_stock', '', false, true, false, false, $callback);
    while ($channel->is_consuming()) {
        $channel->wait();
    }


?>
