<?php
require_once __DIR__ . '/vendor/autoload.php';


use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;



$rabbitHost = "localhost"; 
$rabbitUser = "latch";        
$rabbitPass = "latch";    
$rabbitVhost = "projectVhost";        

$dbHost = "localhost";         
$dbUser = "latch";          
$dbPass = "latch@123";          
$dbName = "auth_db";        




$connection = new AMQPStreamConnection($rabbitHost, 5672, $rabbitUser, $rabbitPass, $rabbitVhost);
$channel = $connection->channel();
$channel->queue_declare('auth_request', false, true, false, false);




$mysqli = new mysqli($dbHost, $dbUser, $dbPass, $dbName);
if ($mysqli->connect_error) {
    die("MySQL connection failed: " . $mysqli->connect_error);
}

echo "[*] Waiting for auth requests...\n";




$callback = function ($msg) use ($channel, $mysqli) {
    echo "[x] Received: ", $msg->body, "\n";

    $data = json_decode($msg->body, true);
    $response = ["status" => "error", "message" => "Invalid request"];

    if ($data && isset($data['action'])) {
        if ($data['action'] === "register") {
            $username = $mysqli->real_escape_string($data['username']);
            $passwordHash = password_hash($data['password'], PASSWORD_BCRYPT);
            $email = $mysqli->real_escape_string($data['email'] ?? "");

            $sql = "INSERT INTO users (username, password_hash, email) VALUES ('$username', '$passwordHash', '$email')";
            if ($mysqli->query($sql)) {
                $response = ["status" => "success", "message" => "User registered"];
            } else {
                $response = ["status" => "error", "message" => "Registration failed: " . $mysqli->error];
            }
        } elseif ($data['action'] === "login") {
            $username = $mysqli->real_escape_string($data['username']);
            $password = $data['password'];

            $sql = "SELECT id, password_hash FROM users WHERE username='$username'";
            $result = $mysqli->query($sql);

            if ($result && $row = $result->fetch_assoc()) {
                if (password_verify($password, $row['password_hash'])) {
                    $sessionKey = bin2hex(random_bytes(16));
                    $userId = $row['id'];

                    $mysqli->query("INSERT INTO sessions (user_id, session_key, expires_at)
                                    VALUES ($userId, '$sessionKey', DATE_ADD(NOW(), INTERVAL 1 HOUR))");

                    $response = ["status" => "success", "session" => $sessionKey];
                } else {
                    $response = ["status" => "error", "message" => "Invalid password"];
                }
            } else {
                $response = ["status" => "error", "message" => "User not found"];
            }
        } elseif ($data['action'] === "validate") {
            $session = $mysqli->real_escape_string($data['session']);

            $sql = "SELECT s.session_key, u.username 
                    FROM sessions s
                    JOIN users u ON s.user_id = u.id
                    WHERE s.session_key = '$session'
                      AND s.expires_at > NOW()
                    LIMIT 1";

            $result = $mysqli->query($sql);

            if ($result && $row = $result->fetch_assoc()) {
                $response = [
                    "status"   => "success",
                    "username" => $row['username']
                ];
            } else {
                $response = [
                    "status"  => "error",
                    "message" => "Invalid or expired session"
                ];
            }
        }
    }



   $msgOut = new AMQPMessage(
        json_encode($response),
        ['correlation_id' => $msg->get('correlation_id')]
    );
    $channel->basic_publish(
        $msgOut,
        '',
        $msg->get('reply_to') ?: 'auth_response'
    );


    echo "[x] Sent response: " . json_encode($response) . "\n";
};

$channel->basic_consume('auth_request', '', false, true, false, false, $callback);




while ($channel->is_consuming()) {
    $channel->wait();
}

$channel->close();
$connection->close();
$mysqli->close();


