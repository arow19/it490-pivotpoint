<?php
require_once __DIR__ . '/vendor/autoload.php';

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

$APIKeyFile = fopen("../doNotPushToGit/polygonAPIKey", "r") or die("Unable to open file!");
$apiKey = trim(fread($APIKeyFile, filesize("../doNotPushToGit/polygonAPIKey")));
fclose($APIKeyFile);
$apiKey = trim($apiKey);

$config = parse_ini_file('/home/andrew/rabbitmqconfig/rabbitconfig.ini');
$rabbitHost = $config['rabbitHost'];
$rabbitUser = $config['rabbitUser'];
$rabbitPass = $config['rabbitPass'];

$vhost  = $config['vhost'];
$connection = new AMQPStreamConnection($rabbitHost, 5672, $rabitUser, $rabbitPass, $vhost);
$ch   = $conn->channel();

$ch->queue_declare('volatility_request',  false, true, false, false);
$ch->queue_declare('volatility_response', false, true, false, false);

echo "Waiting for volatility requests...\n";

$callback = function ($msg) use ($ch, $apiKey) {
    echo "Received request: {$msg->body}\n";
    $request = json_decode($msg->body, true);
    if (!$request || !isset($request['symbol'])) {
        echo "Invalid request payload (expected 'symbol').\n";
        return;
    }

    $symbol = strtoupper($request['symbol']);
    $days   = isset($request['days']) ? (int)$request['days'] : 30;
    $corrId = $msg->get('correlation_id');

    $to   = time() - 86400;
    $from = $to - ($days * 86400);

    $toDate   = date("Y-m-d", $to);
    $fromDate = date("Y-m-d", $from);
    $url = "https://api.polygon.io/v2/aggs/ticker/$symbol/range/1/day/$fromDate/$toDate"
         . "?adjusted=true&sort=asc&limit=5000&apiKey=$apiKey";

    $chCurl = curl_init();
    curl_setopt($chCurl, CURLOPT_URL, $url);
    curl_setopt($chCurl, CURLOPT_RETURNTRANSFER, true);
    $response = curl_exec($chCurl);
    curl_close($chCurl);

    if (!$response) {
        echo "Error fetching data for $symbol\n";
        return;
    }
    $data = json_decode($response, true);
    if (!isset($data['results'])) {
        echo "No sufficient data for $symbol\n";
        return;
    }

    $closes = array_column($data['results'], 'c');
    $returns = [];
    for ($i = 1; $i < count($closes); $i++) {
        $returns[] = log($closes[$i] / $closes[$i - 1]);
    }

    $mean = array_sum($returns) / count($returns);
    $sumSqDiff = 0;
    foreach ($returns as $r) {
        $sumSqDiff += pow($r - $mean, 2);
    }
    $variance = $sumSqDiff / (count($returns) - 1);
    $dailyVolatility = sqrt($variance);
    $annualizedVolatility = $dailyVolatility * sqrt(252);
    $responsePayload = [
        'status' => 'success',
        'symbol' => $symbol,
        'from'   => $fromDate,
        'to'     => $toDate,
        'daily_vol' => round($dailyVolatility, 6),
        'annual_vol' => round($annualizedVolatility, 6),
        'timestamp' => date("Y-m-d H:i:s")
    ];

    $msgOut = new AMQPMessage(json_encode($responsePayload), [
        'content_type' => 'application/json',
        'correlation_id' => $corrId,
        'delivery_mode' => 2
    ]);
    $ch->basic_publish($msgOut, '', 'volatility_response');
    echo "Responded with volatility for {$symbol}\n";
};

$ch->basic_consume('volatility_request', '', false, true, false, false, $callback);
while ($ch->is_consuming()) {
    $ch->wait();
}
?>
