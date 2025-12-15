<?php
require_once __DIR__ . '/vendor/autoload.php';
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

$APIKeyFile = fopen("../doNotPushToGit/finnhubAPIKey", "r") or die("Unable to open file!");
$apiKey = trim(fread($APIKeyFile,filesize("../doNotPushToGit/finnhubAPIKey")));
fclose($APIKeyFile);

$config = parse_ini_file('/home/andrew/rabbitmqconfig/rabbitconfig.ini');
$rabbitHost = $config['rabbitHost'];
$rabbitUser = $config['rabbitUser'];
$rabbitPass = $config['rabbitPass'];
$vhost  = $config['vhost'];
$connection = new AMQPStreamConnection($rabbitHost, 5672, $rabitUser, $rabbitPass, $vhost);
$ch   = $conn->channel();
$ch->queue_declare('request_sell_limit_trade', false, true, false, false);
$ch->queue_declare('response_sell_limit_trade', false, true, false, false);
$ch->queue_declare('request_trade_execution',  false, true, false, false);

$sellLimitOrders = [];

echo "Sell Limit Consumer started...\n";

$callback = function ($msg) use (&$sellLimitOrders, $ch) {
    $data = json_decode($msg->body, true);
    if (!$data || empty($data['symbol']) || empty($data['limit_price'])) {
        echo "Invalid sell limit message: {$msg->body}\n";
        return;
    }

    $data['placed_at'] = time();
    $sellLimitOrders[] = $data;

    echo "Added sell limit order: " . json_encode($data) . "\n";

    $resp = new AMQPMessage(
        json_encode(['status' => 'success', 'message' => 'Sell limit order placed']),
        ['correlation_id' => $msg->get('correlation_id')]
    );
    $ch->basic_publish($resp, '', 'response_sell_limit_trade');
};

$ch->basic_consume('request_sell_limit_trade', '', false, true, false, false, $callback);

$lastCheck = 0;

while (true) {
        $ch->wait(null, false, 1);
    if (time() - $lastCheck >= 60 && count($sellLimitOrders) > 0) {
        echo "Checking " . count($sellLimitOrders) . " active sell limit orders...\n";
        foreach ($sellLimitOrders as $i => $order) {
            $symbol  = strtoupper(trim($order['symbol']));
            $limit   = floatval($order['limit_price']);
            $qty     = floatval($order['quantity']);
            $session = $order['session'];
            $url = "https://finnhub.io/api/v1/quote?symbol={$symbol}&token={$apiKey}";
            $quote = json_decode(@file_get_contents($url), true);
            if (!$quote || !isset($quote['c'])) continue;
            $current = floatval($quote['c']);
            echo "{$symbol}: current={$current}, limit={$limit}\n";
            if ($current >= $limit) {
                echo "Triggering SELL {$symbol} @ {$current} (limit {$limit})\n";
                $exec = [
                    'action'   => 'request_trade_execution',
                    'session'  => $session,
                    'symbol'   => $symbol,
                    'quantity' => $qty,
                    'side'     => 'sell',
                    'price'    => $current
                ];
                $ch->basic_publish(
                    new AMQPMessage(json_encode($exec), ['content_type' => 'application/json']),
                    '',
                    'request_trade_execution'
                );
                unset($sellLimitOrders[$i]);
            }
        }
        $sellLimitOrders = array_values($sellLimitOrders);
        $lastCheck = time();
    }
}
?>
