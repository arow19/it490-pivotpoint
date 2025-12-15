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





$conn = new AMQPStreamConnection($rabbitHost, 5672, $rabbitUser, $rabbitPass, $rabbitVhost);
$ch = $conn->channel();


$ch->queue_declare('portfolio_request', false, true, false, false);
$ch->queue_declare('portfolio_response', false, true, false, false);


$mysqli = new mysqli($dbHost, $dbUser, $dbPass, $dbName);
if ($mysqli->connect_error) {
    die("MySQL connection failed: " . $mysqli->connect_error);
}


echo "[*] Portfolio consumer running and waiting...\n";



$callback = function($msg) use ($ch, $mysqli) {
    echo "[>] Received: " . $msg->body . "\n";
    $data = json_decode($msg->body, true);
    $resp = ["status" => "error", "message" => "Invalid request"];


    if (!$data || !isset($data['action'])) {
        $resp = ["status" => "error", "message" => "Missing action"];
    } else {
        $action  = $data['action'];
        $session = $data['session'] ?? '';
        $amount  = floatval($data['amount'] ?? 0);


 

        $userId = null;
        if ($session) {
            $s = $mysqli->prepare("SELECT user_id FROM sessions WHERE session_key=?");
            $s->bind_param("s", $session);
            $s->execute();
            $res = $s->get_result();
            if ($row = $res->fetch_assoc()) {
                $userId = $row['user_id'];
            }
            $s->close();
        }


        if (!$userId) {
            $resp = ["status" => "error", "message" => "Invalid or expired session"];
        } else {
            switch ($action) {


                     case 'get_portfolio_summary':
                    $q = $mysqli->prepare("SELECT buying_power, total_balance FROM user_portfolio WHERE id=? AND symbol='CASH'");
                    $q->bind_param("i", $userId);
                    $q->execute();
                    $r = $q->get_result();
                    if ($row = $r->fetch_assoc()) {
                        $resp = [
                            "status"        => "success",
                            "buying_power"  => (float)$row['buying_power'],
                            "total_balance" => (float)$row['total_balance']
                        ];
                    } else {
                        $resp = [
                            "status"  => "error",
                            "message" => "No portfolio found for this user"
                        ];
                    }
                    break;


                case 'deposit':
    if ($amount <= 0) {
        $resp = ["status"=>"error","message"=>"Invalid deposit amount"];
        break;
    }


    $check = $mysqli->prepare("SELECT 1 FROM user_portfolio WHERE id=? AND symbol='CASH'");
    $check->bind_param("i", $userId);
    $check->execute();
    $check->store_result();


    if ($check->num_rows === 0) {


        $insert = $mysqli->prepare("
            INSERT INTO user_portfolio (id, stock, symbol, quantity, price, buying_power, total_balance)
            VALUES (?, 'Cash Balance', 'CASH', 0, 1, 0, 0)
        ");
        $insert->bind_param("i", $userId);
        $insert->execute();
    }
    $check->close();
                    $u = $mysqli->prepare("
                        UPDATE user_portfolio
                        SET buying_power = buying_power + ?, total_balance = total_balance + ?
                        WHERE id=? AND symbol='CASH'
                    ");
                    $u->bind_param("ddi", $amount, $amount, $userId);
                    $u->execute();


                    if ($u->affected_rows === 0) {
                        $resp = ["status"=>"error","message"=>"No CASH balance found for this user"];
                        break;
                    }


                    $q = $mysqli->prepare("SELECT buying_power,total_balance FROM user_portfolio WHERE id=? AND symbol='CASH'");
                    $q->bind_param("i", $userId);
                    $q->execute();
                    $r = $q->get_result();
                    $row = $r->fetch_assoc();


            
                    $log = $mysqli->prepare("
                        INSERT INTO user_trades (id, symbol, trade_type, quantity, price)
                        VALUES (?, 'CASH', 'BUY', 0, ?)
                    ");
                    $log->bind_param("id", $userId, $amount);
                    $log->execute();


                    $resp = [
                        "status"        => "success",
                        "message"       => "Deposit successful",
                        "buying_power"  => (float)$row['buying_power'],
                        "total_balance" => (float)$row['total_balance']
                    ];
                    break;


  
                case 'get_holdings':
                    $q = $mysqli->prepare("
                        SELECT p.stock, p.symbol, p.quantity, p.price, c.price AS current_price
                        FROM user_portfolio p
                        LEFT JOIN stocks_cache c ON p.symbol = c.symbol
                        WHERE p.id=? AND p.symbol <> 'CASH'
                    ");
                    $q->bind_param("i", $userId);
                    $q->execute();
                    $r = $q->get_result();


                    $holdings = [];
                    while ($h = $r->fetch_assoc()) {
                        $holdings[] = [
                            "stock"         => $h['stock'],
                            "symbol"        => $h['symbol'],
                            "quantity"      => (float)$h['quantity'],
                            "price"         => (float)$h['price'],           
                            "current_price" => (float)($h['current_price'] ?? $h['price'])
                        ];
                    }


                    $resp = ["status" => "success", "holdings" => $holdings];
                    break;


                default:
                    $resp = ["status" => "error", "message" => "Unknown action: {$action}"];
                    break;
            }
        }
    }


    $out = new AMQPMessage(json_encode($resp), [
        'correlation_id' => $msg->get('correlation_id')
    ]);
    $replyTo = $msg->get('reply_to') ?: 'portfolio_response';
    $ch->basic_publish($out, '', $replyTo);


    echo "[<] Sent response: " . json_encode($resp) . "\n";
};


$ch->basic_consume('portfolio_request', '', false, true, false, false, $callback);
while ($ch->is_consuming()) {
    $ch->wait();
}


$ch->close();
$conn->close();
$mysqli->close();
?>
