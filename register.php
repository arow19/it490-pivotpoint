<?php
require_once __DIR__ . '/vendor/autoload.php';

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

$config = parse_ini_file('/var/www/sample/rabbitconfig.txt');
$rabbitHost = $config['rabbitHost'];
$rabbitUser = $config['rabbitUser'];
$rabbitPass = $config['rabbitPass'];
$vhost  = $config['vhost'];

$connection = new AMQPStreamConnection($rabbitHost, 5672, $rabbitUser, $rabbitPass, $vhost);
$channel = $connection->channel();

$channel->queue_declare('auth_request', false, true, false, false);

list($callbackQueue,,) = $channel->queue_declare("", false, false, true, false);

$username = $_POST['uname'] ?? '';
$password = $_POST['pword'] ?? '';
$email    = $_POST['email'] ?? '';

if (!$username || !$password || !$email) {
    echo json_encode(["status" => "error", "message" => "All fields required"]);
    exit;
}

$data = [
    "action"   => "register",
    "username" => $username,
    "password" => $password,
    "email"    => $email
];

$corrId = uniqid();

$msg = new AMQPMessage(
    json_encode($data),
    [
        'correlation_id' => $corrId,
        'reply_to' => $callbackQueue
    ]
);

$channel->basic_publish($msg, '', 'auth_request');

$response = null;
$callback = function($msg) use (&$response, $corrId) {
    if ($msg->get('correlation_id') === $corrId) {
        $response = $msg->body;
    }
};
$channel->basic_consume($callbackQueue, '', false, true, false, false, $callback);

$start = time();
while ($response === null && (time() - $start) < 5) {
    $channel->wait(null, false, 1);
}
if ($response === null) {
    $response = json_encode(["status" => "error", "message" => "No response from DB"]);
}
header("Content-Type: application/json");
echo $response;

$channel->close();
$connection->close();
?>

