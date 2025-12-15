<?php
require_once __DIR__ . '/vendor/autoload.php';

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

$APIKeyFile = fopen("../doNotPushToGit/finnhubAPIKey", "r") or die("Unable to open file!");
$finnhubKey = trim(fread($APIKeyFile,filesize("../doNotPushToGit/finnhubAPIKey")));
fclose($APIKeyFile);
$config = parse_ini_file('/home/andrew/rabbitmqconfig/rabbitconfig.ini');
$rabbitHost = $config['rabbitHost'];
$rabbitUser = $config['rabbitUser'];
$rabbitPass = $config['rabbitPass'];
$vhost  = $config['vhost'];

$connection = new AMQPStreamConnection($rabbitHost, 5672, $rabitUser, $rabbitPass, $vhost);
$channel = $conn->channel();
$channel->queue_declare('stock_requests', false, true, false, false);
$channel->queue_declare('stock_updates', false, true, false, false);

echo "Waiting for stock request...\n";
$callback = function ($msg) use ($channel, $finnhubKey) {
$corrId = $msg->get('correlation_id');
$replyQueue = $msg->get('reply_to') ?: 'stock_updates';

$body = $msg->body;
$data = json_decode($body, true);

echo "\nReceived message: $body\n";

$response = ['status' => 'error', 'message' => 'Invalid request'];

if ($data && isset($data['action']) && $data['action'] === 'fetch_stock') {
    $symbol = strtoupper(trim($data['symbol'] ?? ''));

    if ($symbol === '') {
        $response = ['status' => 'error', 'message' => 'No symbol provided'];
        echo "No symbol provided.\n";
    } else {
        $url = "https://finnhub.io/api/v1/quote?symbol=$symbol&token=$finnhubKey";
        echo "Fetching $symbol from Finnhub...\n";

        $opts = [
            "http" => [
                "timeout" => 5,
                "ignore_errors" => true,
                "header" => "User-Agent: PivotPointDMZ/1.0\r\n"
            ]
        ];
        $context = stream_context_create($opts);
        $result = @file_get_contents($url, false, $context);

        if ($result === false) {
            $response = ['status' => 'error', 'message' => 'Finnhub unreachable'];
            echo "Finnhub unreachable for $symbol.\n";
        } else {
            $quote = json_decode($result, true);
            if (isset($quote['c'])) {
                $response = [
                    'status'  => 'success',
                    'symbol'  => $symbol,
                    'price'   => $quote['c'],
                    'change'  => $quote['d'] ?? 0,
                    'percent' => $quote['dp'] ?? 0
                ];
                echo "$symbol => Price: {$quote['c']} Change: {$quote['d']} ({$quote['dp']}%)\n";
            } else {
                $response = ['status' => 'error', 'message' => 'Invalid API data'];
                echo "Invalid API response for $symbol.\n";
            }
        }
    }
} else {
    echo "Unknown or missing action field.\n";
}
$outMsg = new AMQPMessage(
    json_encode($response),
    [
        'correlation_id' => $corrId,
        'content_type' => 'application/json'
    ]
);

$channel->basic_publish($outMsg, '', $replyQueue);
echo "Sent response: " . json_encode($response) . "\n";
};
$channel->basic_consume('stock_requests', '', false, true, false, false, $callback);

while (true) {
        $channel->wait();
}
?>
