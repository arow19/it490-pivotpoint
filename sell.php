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


    echo "[DB] Waiting for messages on 'request_trade_execution'...\n";


    $callback = function ($msg) use ($ch, $dbHost, $dbUser, $dbPass, $dbName) {
        $payload = json_decode($msg->body, true);
        echo "[DB] Received trade request: {$msg->body}\n";


        $response = ['status' => 'error', 'message' => 'Unknown error'];


        if (!is_array($payload) || empty($payload['session']) || empty($payload['symbol']) || empty($payload['side'])) {
            $response['message'] = 'Invalid payload structure';
        } else {
            $symbol   = strtoupper(trim($payload['symbol']));
            $side     = strtolower(trim($payload['side'])); // 'buy' or 'sell'
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


                    $buyingPower  = floatval($cash['buying_power']);
                    $totalBalance = floatval($cash['total_balance']);


                    if ($side === 'buy') {
                        if ($buyingPower < $total) {
                            $response['message'] = "Insufficient buying power";
                        } else {
                            $mysqli->begin_transaction();
                            try {
                                $upd = $mysqli->prepare("UPDATE user_portfolio 
                                    SET buying_power = buying_power - ?, total_balance = total_balance - ? 
                                    WHERE id=? AND symbol='CASH'");
                                $upd->bind_param("ddi", $total, $total, $uid);
                                $upd->execute();
                                $upd->close();

                                $pos = $mysqli->prepare("SELECT quantity, price FROM user_portfolio WHERE id=? AND symbol=?");
                                $pos->bind_param("is", $uid, $symbol);
                                $pos->execute();
                                $exist = $pos->get_result()->fetch_assoc();
                                $pos->close();


                                if ($exist) {
                                    $oldQty = $exist['quantity'];
                                    $oldPrice = $exist['price'];
                                    $newQty = $oldQty + $qty;
                                    $newAvg = (($oldQty * $oldPrice) + ($qty * $price)) / $newQty;
                                    $upd = $mysqli->prepare("UPDATE user_portfolio SET quantity=?, price=? WHERE id=? AND symbol=?");
                                    $upd->bind_param("ddis", $newQty, $newAvg, $uid, $symbol);
                                    $upd->execute();
                                    $upd->close();
                                } else {
                                    $ins = $mysqli->prepare("INSERT INTO user_portfolio (id, stock, symbol, quantity, price) VALUES (?, ?, ?, ?, ?)");
                                    $ins->bind_param("issdd", $uid, $symbol, $symbol, $qty, $price);
                                    $ins->execute();
                                    $ins->close();
                                }


                                $mysqli->commit();
                                $response = ['status'=>'success', 'message'=>"Bought {$qty} shares of {$symbol} at {$price}"];
                            } catch (Throwable $e) {
                                $mysqli->rollback();
                                $response = ['status'=>'error','message'=>'Buy failed: '.$e->getMessage()];
                            }
                        }
                    } elseif ($side === 'sell') {
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
                                    'status'=>'success',
                                    'message'=>"Sold {$qty} shares of {$symbol} at {$price}. Profit: ".number_format($profit,2),
                                    'profit'=>$profit
                                ];
                            } catch (Throwable $e) {
                                $mysqli->rollback();
                                $response = ['status'=>'error','message'=>'Sell failed: '.$e->getMessage()];
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
        $corrId  = $msg->get('correlation_id');


        $ch->basic_publish(
            new AMQPMessage(json_encode($response), [
                'correlation_id' => $corrId,
                'content_type'   => 'application/json'
            ]),
            '',
            $replyTo
        );


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

