<?php
require_once __DIR__ . '/vendor/autoload.php';

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

$rabbitHost = 'localhost';
$rabbitUser = 'latch';
$rabbitPass = 'latch';
$vhost      = 'projectVhost';

$dbHost = 'localhost';
$dbUser = 'latch';
$dbPass = 'latch@123';
$dbName = 'auth_db';

echo "[DB] Starting trade execution consumer...\n";

try {
    $conn = new AMQPStreamConnection($rabbitHost, 5672, $rabbitUser, $rabbitPass, $vhost);
    $ch   = $conn->channel();

    $ch->queue_declare('request_trade_execution', false, true, false, false);
    $ch->queue_declare('response_trade_execution', false, true, false, false);
    $ch->queue_declare('request_email_notification', false, true, false, false);

    echo "[DB] Waiting for messages on 'request_trade_execution'...\n";

    $callback = function ($msg) use ($ch, $dbHost, $dbUser, $dbPass, $dbName) {
        $payload = json_decode($msg->body, true);
        echo "[DB] Received trade request: {$msg->body}\n";

        $response = ['status' => 'error', 'message' => 'Unknown error'];

        if (!is_array($payload) || empty($payload['session']) || empty($payload['symbol']) || empty($payload['side'])) {
            $response['message'] = 'Invalid payload structure';
        } else {
            $symbol   = strtoupper(trim($payload['symbol']));
            $side     = strtolower(trim($payload['side']));
            $qty      = floatval($payload['quantity'] ?? 0);
            $price    = floatval($payload['price'] ?? 0);
            $session  = $payload['session'];
            $total    = $qty * $price;

            $mysqli = new mysqli($dbHost, $dbUser, $dbPass, $dbName);
            if ($mysqli->connect_errno) {
                $response['message'] = "DB connection failed: {$mysqli->connect_error}";
            } else {
                $stmt = $mysqli->prepare("SELECT user_id FROM sessions WHERE session_key=?");
                $stmt->bind_param("s", $session);
                $stmt->execute();
                $row = $stmt->get_result()->fetch_assoc();
                $stmt->close();

                if (!$row) {
                    $response['message'] = "Invalid session";
                } else {
                    $uid = intval($row['user_id']);

                    $mysqli->query("INSERT IGNORE INTO user_portfolio (id, stock, symbol, quantity, price, buying_power, total_balance)
                                    VALUES ($uid, 'Cash Balance', 'CASH', 0, 1, 10000, 10000)");

                    $cashQ = $mysqli->prepare("SELECT buying_power, total_balance FROM user_portfolio WHERE id=? AND symbol='CASH'");
                    $cashQ->bind_param("i", $uid);
                    $cashQ->execute();
                    $cash = $cashQ->get_result()->fetch_assoc();
                    $cashQ->close();

                    $buyingPower  = floatval($cash['buying_power'] ?? 0);
                    $totalBalance = floatval($cash['total_balance'] ?? 0);

                    if ($side === 'buy') {
                        if ($buyingPower < $total) {
                            $response['message'] = "Insufficient buying power";
                        } else {
                            try {
                                $mysqli->begin_transaction();
                                $insCache = $mysqli->prepare("
                                    INSERT INTO stocks_cache (symbol, name, price, change_val, change_percent, updated_at)
                                    VALUES (?, ?, ?, 0, 0, NOW())
                                    ON DUPLICATE KEY UPDATE price=VALUES(price), updated_at=VALUES(updated_at)
                                ");
                                $name = $symbol;
                                $insCache->bind_param("ssd", $symbol, $name, $price);
                                $insCache->execute();
                                $insCache->close();
                                $mysqli->commit();
                                $mysqli->query("DO SLEEP(0.05)");
                            } catch (Throwable $e) {
                                $mysqli->rollback();
                                echo "[DB] Cache insert failed for $symbol: {$e->getMessage()}\n";
                            }

                            
                            $mysqli->close();
                            $mysqli = new mysqli($dbHost, $dbUser, $dbPass, $dbName);
                            if ($mysqli->connect_errno) {
                                $response['message'] = "Reconnection failed: {$mysqli->connect_error}";
                            } else {
                                $mysqli->begin_transaction();
                                try {
                                    $upd = $mysqli->prepare("
                                        UPDATE user_portfolio 
                                           SET buying_power = buying_power - ?, 
                                               total_balance = total_balance - ? 
                                         WHERE id=? AND symbol='CASH'
                                    ");
                                    $upd->bind_param("ddi", $total, $total, $uid);
                                    $upd->execute();
                                    $upd->close();

                                    $pos = $mysqli->prepare("SELECT quantity, price FROM user_portfolio WHERE id=? AND symbol=?");
                                    $pos->bind_param("is", $uid, $symbol);
                                    $pos->execute();
                                    $exist = $pos->get_result()->fetch_assoc();
                                    $pos->close();

                                    if ($exist) {
                                        $oldQty   = (float)$exist['quantity'];
                                        $oldPrice = (float)$exist['price'];
                                        $newQty   = $oldQty + $qty;
                                        $newAvg   = ($oldQty * $oldPrice + $qty * $price) / ($newQty ?: 1);
                                        $upd = $mysqli->prepare("UPDATE user_portfolio SET quantity=?, price=? WHERE id=? AND symbol=?");
                                        $upd->bind_param("ddis", $newQty, $newAvg, $uid, $symbol);
                                        $upd->execute();
                                        $upd->close();
                                    } else {
                                        $ins = $mysqli->prepare("
                                            INSERT INTO user_portfolio (id, stock, symbol, quantity, price)
                                            VALUES (?, ?, ?, ?, ?)
                                        ");
                                        $ins->bind_param("issdd", $uid, $symbol, $symbol, $qty, $price);
                                        $ins->execute();
                                        $ins->close();
                                    }

                                    $mysqli->commit();

                                    
                                    $confirm = $mysqli->prepare("SELECT price FROM stocks_cache WHERE symbol=?");
                                    $confirm->bind_param("s", $symbol);
                                    $confirm->execute();
                                    $confirmed = $confirm->get_result()->fetch_assoc();
                                    $confirm->close();

                                    if (!empty($confirmed['price'])) {
                                        $price = (float)$confirmed['price'];
                                    }

                                    $sel = $mysqli->prepare("SELECT buying_power FROM user_portfolio WHERE id=? AND symbol='CASH'");
                                    $sel->bind_param("i", $uid);
                                    $sel->execute();
                                    $bpRow = $sel->get_result()->fetch_assoc();
                                    $sel->close();
                                    $newBP = (float)($bpRow['buying_power'] ?? 0);

                                    $response = [
                                        'status'       => 'success',
                                        'message'      => "Bought {$qty} shares of {$symbol} at {$price}",
                                        'shares'       => (float)$qty,
                                        'symbol'       => $symbol,
                                        'price'        => (float)$price,
                                        'buying_power' => $newBP
                                    ];

                                    
                                    $emailFind = $mysqli->query("SELECT email FROM users WHERE id=$uid");
                                    $email = $emailFind->fetch_assoc()['email'] ?? '';
                                    if ($email) {
                                        $emailPayload = [
                                            'action'      => 'send_trade_email',
                                            'email'       => $email,
                                            'user_id'     => $uid,
                                            'trade_type'  => 'BUY',
                                            'symbol'      => $symbol,
                                            'quantity'    => $qty,
                                            'price'       => $price,
                                            'timestamp'   => date('Y-m-d H:i:s')
                                        ];
                                        $ch->basic_publish(new AMQPMessage(json_encode($emailPayload)), '', 'request_email_notification');
                                        echo "[DB] Queued email for BUY -> $email\n";
                                    }

                                } catch (Throwable $e) {
                                    $mysqli->rollback();
                                    $response = ['status' => 'error', 'message' => 'Buy failed: ' . $e->getMessage()];
                                }
                            } 
                        } 
                    } 

                    
                    elseif ($side === 'sell') { 
                        $pos = $mysqli->prepare("SELECT quantity, price FROM user_portfolio WHERE id=? AND symbol=?");
                        $pos->bind_param("is", $uid, $symbol);
                        $pos->execute();
                        $exist = $pos->get_result()->fetch_assoc();
                        $pos->close();

                        if (!$exist) {
                            $response['message'] = "No holdings for {$symbol}";
                        } elseif ($exist['quantity'] < $qty) {
                            $response['message'] = "Not enough shares to sell";
                        } else {
                            $mysqli->begin_transaction();
                            try {
                                $newQty = $exist['quantity'] - $qty;
                                $profit = ($price - $exist['price']) * $qty;

                                if ($newQty > 0) {
                                    $upd = $mysqli->prepare("UPDATE user_portfolio SET quantity=? WHERE id=? AND symbol=?");
                                    $upd->bind_param("dis", $newQty, $uid, $symbol);
                                    $upd->execute();
                                    $upd->close();
                                } else {
                                    $del = $mysqli->prepare("DELETE FROM user_portfolio WHERE id=? AND symbol=?");
                                    $del->bind_param("is", $uid, $symbol);
                                    $del->execute();
                                    $del->close();
                                }

                                $upd = $mysqli->prepare("UPDATE user_portfolio 
                                    SET buying_power = buying_power + ?, total_balance = total_balance + ? 
                                    WHERE id=? AND symbol='CASH'");
                                $upd->bind_param("ddi", $total, $profit, $uid);
                                $upd->execute();
                                $upd->close();

                                $mysqli->commit();

                                $response = [
                                    'status'  => 'success',
                                    'message' => "Sold {$qty} shares of {$symbol} at {$price}. Profit: " . number_format($profit, 2),
                                    'profit'  => $profit
                                ];

                                $emailRes = $mysqli->query("SELECT email FROM users WHERE id=$uid");
                                $email = $emailRes->fetch_assoc()['email'] ?? '';
                                if ($email) {
                                    $emailPayload = [
                                        'action'      => 'send_trade_email',
                                        'email'       => $email,
                                        'user_id'     => $uid,
                                        'trade_type'  => 'SELL',
                                        'symbol'      => $symbol,
                                        'quantity'    => $qty,
                                        'price'       => $price,
                                        'profit_loss' => $profit,
                                        'timestamp'   => date('Y-m-d H:i:s')
                                    ];
                                    $ch->basic_publish(new AMQPMessage(json_encode($emailPayload)), '', 'request_email_notification');
                                    echo "[DB] Queued email for SELL -> $email\n";
                                }

                            } catch (Throwable $e) {
                                $mysqli->rollback();
                                $response = ['status' => 'error', 'message' => 'Sell failed: ' . $e->getMessage()];
                            }
                        }
                    } else {
                        $response['message'] = "Invalid trade side";
                    }

                    $mysqli->close();
                } 
            } 
        } 
        $replyTo = $msg->get('reply_to') ?: 'response_trade_execution';
        $corrId  = $msg->get('correlation_id') ?? '';

        $responseMsg = new AMQPMessage(json_encode($response), [
            'correlation_id' => $corrId,
            'content_type'   => 'application/json'
        ]);

        $ch->basic_publish($responseMsg, '', $replyTo);
        echo "[DB] Published response to $replyTo\n";
        echo "[DB] Trade handled: " . json_encode($response) . "\n";
    };

    $ch->basic_consume('request_trade_execution', '', false, true, false, false, $callback);
    while ($ch->is_consuming()) {
        $ch->wait();
    }

} catch (Throwable $e) {
    echo "[DB] ERROR: {$e->getMessage()}\n";
    exit(1);
}

