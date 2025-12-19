<?php
//deploy test 8
require_once __DIR__ . '/vendor/autoload.php';
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    header('Content-Type: application/json');

    $raw     = file_get_contents('php://input');
    $payload = json_decode($raw, true);
    $action  = $payload['action'] ?? '';
    $session = $payload['session'] ?? '';

    if ($action === 'validate') {
        if (!$session) {
            http_response_code(400);
            echo json_encode(['status' => 'error', 'message' => 'No session provided.']);
            exit;
        }
	try {
	    $config = parse_ini_file('/var/www/sample/rabbitconfig.txt');
   	    $rabbitHost = $config['rabbitHost'];
   	    $rabbitUser = $config['rabbitUser'];
            $rabbitPass = $config['rabbitPass'];
  	    $vhost  = $config['vhost'];

            $connection = new AMQPStreamConnection($rabbitHost, 5672, $rabbitUser, $rabbitPass, $vhost);

            $channel    = $connection->channel();
            $channel->queue_declare('auth_request', false, true, false, false);
            $channel->queue_declare('auth_response', false, true, false, false);

            $corrId = bin2hex(random_bytes(12));
            $data   = ['action' => 'validate', 'session' => $session];
            $msg    = new AMQPMessage(json_encode($data), [
                'correlation_id' => $corrId,
                'reply_to'       => 'auth_response',
                'content_type'   => 'application/json'
            ]);

            $channel->basic_publish($msg, '', 'auth_request');

            $response = null;
            $callback = function ($msg) use (&$response, $corrId) {
                if ($msg->get('correlation_id') === $corrId) {
                    $response = $msg->body;
                }
            };
            $channel->basic_consume('auth_response', '', false, true, false, false, $callback);

            $start = time();
            while ($response === null && (time() - $start) < 5) {
                $channel->wait(null, false, 1);
            }

            $channel->close();
            $connection->close();

            if ($response === null) {
                http_response_code(504);
                echo json_encode(['status' => 'error', 'message' => 'No response from auth server.']);
                exit;
            }

            $result = json_decode($response, true);
            if (!$result || ($result['status'] ?? '') !== 'success') {
                http_response_code(401);
                echo json_encode(['status' => 'error', 'message' => 'Invalid or expired session.']);
                exit;
            }

            echo json_encode(['status' => 'success', 'user' => $result['user'] ?? null]);
            exit;
        } catch (Throwable $e) {
            http_response_code(500);
            echo json_encode(['status' => 'error', 'message' => 'Server error', 'detail' => $e->getMessage()]);
            exit;
        }
    }

    if ($action === 'get_portfolio_summary') {
        try {
            $config = parse_ini_file('/var/www/sample/rabbitconfig.txt');
            $rabbitHost = $config['rabbitHost'];
            $rabbitUser = $config['rabbitUser'];
            $rabbitPass = $config['rabbitPass'];
            $vhost  = $config['vhost'];

            $conn = new AMQPStreamConnection($rabbitHost, 5672, $rabbitUser, $rabbitPass, $vhost);

            $ch   = $conn->channel();
            $ch->queue_declare('portfolio_request', false, true, false, false);
            $ch->queue_declare('portfolio_response', false, true, false, false);

            $corrId = bin2hex(random_bytes(12));
            $msg    = new AMQPMessage(json_encode(['action' => 'get_portfolio_summary', 'session' => $session]), [
                'correlation_id' => $corrId,
                'reply_to'       => 'portfolio_response',
                'content_type'   => 'application/json'
            ]);

            $ch->basic_publish($msg, '', 'portfolio_request');

            $response = null;
            $callback = function ($m) use (&$response, $corrId) {
                if ($m->get('correlation_id') === $corrId) {
                    $response = $m->body;
                }
            };
            $ch->basic_consume('portfolio_response', '', false, true, false, false, $callback);

            $start = time();
            while ($response === null && (time() - $start) < 20) {
                $ch->wait(null, false, 1);
            }

            $ch->close();
            $conn->close();

            if ($response === null) {
                http_response_code(504);
                echo json_encode(['status' => 'error', 'message' => 'No portfolio response']);
                exit;
            }

            echo $response;
            exit;
        } catch (Throwable $e) {
            http_response_code(500);
            echo json_encode(['status' => 'error', 'message' => 'Server error', 'detail' => $e->getMessage()]);
            exit;
        }
    }

    if ($action === 'deposit') {
        $amount = floatval($payload['amount'] ?? 0);
        if (!$session || $amount <= 0) {
            http_response_code(400);
            echo json_encode(['status' => 'error', 'message' => 'Invalid session or amount.']);
            exit;
        }
	try {
	    $config = parse_ini_file('/var/www/sample/rabbitconfig.txt');
            $rabbitHost = $config['rabbitHost'];
            $rabbitUser = $config['rabbitUser'];
            $rabbitPass = $config['rabbitPass'];
            $vhost  = $config['vhost'];

	    $conn = new AMQPStreamConnection($rabbitHost, 5672, $rabbitUser, $rabbitPass, $vhost);
            $ch   = $conn->channel();
            $ch->queue_declare('portfolio_request', false, true, false, false);
            $ch->queue_declare('portfolio_response', false, true, false, false);

            $corrId = bin2hex(random_bytes(12));
            $msg    = new AMQPMessage(json_encode(['action' => 'deposit', 'session' => $session, 'amount' => $amount]), [
                'correlation_id' => $corrId,
                'reply_to'       => 'portfolio_response',
                'content_type'   => 'application/json'
            ]);

            $ch->basic_publish($msg, '', 'portfolio_request');

            $response = null;
            $callback = function ($m) use (&$response, $corrId) {
                if ($m->get('correlation_id') === $corrId) {
                    $response = $m->body;
                }
            };
            $ch->basic_consume('portfolio_response', '', false, true, false, false, $callback);

            $start = time();
            while ($response === null && (time() - $start) < 8) {
                $ch->wait(null, false, 1);
            }

            $ch->close();
            $conn->close();

            echo $response ?? json_encode(['status' => 'error', 'message' => 'No response from portfolio service']);
            exit;
        } catch (Throwable $e) {
            http_response_code(500);
            echo json_encode(['status' => 'error', 'message' => 'Server error', 'detail' => $e->getMessage()]);
            exit;
        }
    }

    if ($action === 'buy_stock') {
        $symbol = strtoupper(trim($payload['symbol'] ?? ''));
        $shares = intval($payload['shares'] ?? 0);
        $price  = floatval($payload['price'] ?? 0);

        if (!$session || !$symbol || $shares <= 0 || $price <= 0) {
            http_response_code(400);
            echo json_encode(['status' => 'error', 'message' => 'Invalid trade parameters (whole shares & price required).']);
            exit;
        }

        try {
            $config = parse_ini_file('/var/www/sample/rabbitconfig.txt');
            $rabbitHost = $config['rabbitHost'];
            $rabbitUser = $config['rabbitUser'];
            $rabbitPass = $config['rabbitPass'];
            $vhost  = $config['vhost'];

            $conn = new AMQPStreamConnection($rabbitHost, 5672, $rabbitUser, $rabbitPass, $vhost);

            $ch   = $conn->channel();
            $ch->queue_declare('request_trade_execution', false, true, false, false);
            $ch->queue_declare('response_trade_execution', false, true, false, false);

            $corrId = bin2hex(random_bytes(12));
            $msg    = new AMQPMessage(json_encode([
                'action'   => 'buy_stock',
                'session'  => $session,
                'symbol'   => $symbol,
                'quantity' => $shares,
                'price'    => $price,
                'side'     => 'buy'
            ]), [
                'correlation_id' => $corrId,
                'reply_to'       => 'response_trade_execution',
                'content_type'   => 'application/json'
            ]);

            $ch->basic_publish($msg, '', 'request_trade_execution');

            $response = null;
            $callback = function ($m) use (&$response, $corrId) {
                if ($m->get('correlation_id') === $corrId) {
                    $response = $m->body;
                }
            };
            $ch->basic_consume('response_trade_execution', '', false, true, false, false, $callback);

            $start = time();
            while ($response === null && (time() - $start) < 30) {
                $ch->wait(null, false, 1);
            }

            $ch->close();
            $conn->close();

            if ($response === null) {
                http_response_code(504);
                echo json_encode(['status' => 'error', 'message' => 'No response from transaction service']);
                exit;
            }

            echo $response;
            exit;
        } catch (Throwable $e) {
            http_response_code(500);
            echo json_encode(['status' => 'error', 'message' => 'Server error', 'detail' => $e->getMessage()]);
            exit;
        }
    }

    if ($action === 'request_buy_limit_trade') {
        $symbol     = strtoupper(trim($payload['symbol'] ?? ''));
        $quantity   = floatval($payload['quantity'] ?? 0);
        $limitPrice = floatval($payload['limit_price'] ?? 0);
        $side       = $payload['side'] ?? 'buy';

        if (!$symbol || $quantity <= 0 || $limitPrice <= 0 || !$session) {
            http_response_code(400);
            echo json_encode(['status' => 'error', 'message' => 'Invalid buy limit parameters']);
            exit;
        }

        try {
            $config = parse_ini_file('/var/www/sample/rabbitconfig.txt');
            $rabbitHost = $config['rabbitHost'];
            $rabbitUser = $config['rabbitUser'];
            $rabbitPass = $config['rabbitPass'];
            $vhost  = $config['vhost'];

            $conn = new AMQPStreamConnection($rabbitHost, 5672, $rabbitUser, $rabbitPass, $vhost);

            $ch   = $conn->channel();
            $ch->queue_declare('request_buy_limit_trade', false, true, false, false);
            $ch->queue_declare('response_buy_limit_trade', false, true, false, false);

            $corrId = bin2hex(random_bytes(12));
            $msg    = new AMQPMessage(json_encode([
                'action'      => $action,
                'session'     => $session,
                'symbol'      => $symbol,
                'quantity'    => $quantity,
                'limit_price' => $limitPrice,
                'side'        => $side
            ]), [
                'correlation_id' => $corrId,
                'reply_to'       => 'response_buy_limit_trade',
                'content_type'   => 'application/json'
            ]);

            $ch->basic_publish($msg, '', 'request_buy_limit_trade');

            $response = null;
            $callback = function ($m) use (&$response, $corrId) {
                if ($m->get('correlation_id') === $corrId) {
                    $response = $m->body;
                }
            };
            $ch->basic_consume('response_buy_limit_trade', '', false, true, false, false, $callback);

            $start = time();
            while ($response === null && (time() - $start) < 6) {
                $ch->wait(null, false, 1);
            }

            $ch->close();
            $conn->close();

            if ($response === null) {
                http_response_code(504);
                echo json_encode(['status' => 'error', 'message' => 'No response from buy limit service']);
                exit;
            }

            echo $response;
            exit;
        } catch (Throwable $e) {
            http_response_code(500);
            echo json_encode(['status' => 'error', 'message' => 'Server error', 'detail' => $e->getMessage()]);
            exit;
        }
    }

    if ($action === 'fetch_stock' || $action === 'fetch_popular') {
        $symbols = [];
        if ($action === 'fetch_stock') {
            $sym = strtoupper(trim($payload['symbol'] ?? ''));
            if (!$sym) {
                http_response_code(400);
                echo json_encode(['status' => 'error', 'message' => 'No symbol provided']);
                exit;
            }
            $symbols[] = $sym;
        } else {
            $symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA', 'NFLX', 'AMD', 'INTC'];
        }

        try {
            $config = parse_ini_file('/var/www/sample/rabbitconfig.txt');
            $rabbitHost = $config['rabbitHost'];
            $rabbitUser = $config['rabbitUser'];
            $rabbitPass = $config['rabbitPass'];
            $vhost  = $config['vhost'];

            $conn = new AMQPStreamConnection($rabbitHost, 5672, $rabbitUser, $rabbitPass, $vhost);

            $ch   = $conn->channel();
            $ch->queue_declare('stock_requests', false, true, false, false);
            $ch->queue_declare('stock_updates', false, true, false, false);

            $results = [];
            $pending = [];

            foreach ($symbols as $sym) {
                $corrId = bin2hex(random_bytes(12));
                $data   = ['action' => 'fetch_stock', 'symbol' => $sym];
                $msg    = new AMQPMessage(json_encode($data), [
                    'correlation_id' => $corrId,
                    'reply_to'       => 'stock_updates',
                    'content_type'   => 'application/json'
                ]);
                $pending[$corrId] = $sym;
                $ch->basic_publish($msg, '', 'stock_requests');
            }

            $callback = function ($msg) use (&$results, &$pending) {
                $cid = $msg->get('correlation_id');
                if (isset($pending[$cid])) {
                    $results[] = json_decode($msg->body, true);
                    unset($pending[$cid]);
                }
            };
            $ch->basic_consume('stock_updates', '', false, true, false, false, $callback);

            $start = time();
            while (!empty($pending) && (time() - $start) < 8) {
                $ch->wait(null, false, 1);
            }

            $ch->close();
            $conn->close();

            if ($action === 'fetch_stock') {
                echo json_encode($results[0] ?? ['status' => 'error', 'message' => 'No stock response']);
            } else {
                echo json_encode(['status' => 'success', 'data' => $results]);
            }
            exit;
        } catch (Throwable $e) {
            http_response_code(500);
            echo json_encode(['status' => 'error', 'message' => 'Server error', 'detail' => $e->getMessage()]);
            exit;
        }
    }
}
?>

<html>
<head>
  <title>Pivot Point</title>
  <meta name="viewport" content="width=device-width, initial=scale=1.0">
  <style>
    body { font-family: Arial, sans-serif; background: #eef2f7; margin: 0; padding: 0; }
    header { background: #007bff; color: white; padding: 15px 20px; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px; }
    main { padding: 25px; }
    .hidden { display: none; }
    table { width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
    th, td { padding: 10px; text-align: center; border-bottom: 1px solid #ddd; }
    th { background: #007bff; color: white; }
    button { padding: 8px 14px; background: #007bff; color: white; border: none; border-radius: 5px; cursor: pointer; }
    button:hover { background: #0056b3; }
    input { padding: 8px; border: 1px solid #ccc; border-radius: 4px; }
    .balance-box { background: rgba(255,255,255,0.15); padding: 6px 10px; border-radius: 6px; font-weight: bold; }
  </style>
</head>
<body>
  <header>
    <h1>Welcome to Pivot Point v8</h1>
    <div>
      <span class="balance-box">Buying Power: <span id="bpValue">$0.00</span></span>
      <button id="depositBtn">Deposit</button>
      <button id="portfolioBtn">Portfolio</button>
      <button id="logoutBtn">Logout</button>
    </div>
  </header>

  <main>
    <div id="statusText">Validating session…</div>

    <div id="authedArea" class="hidden">
      <h2>Popular Stocks</h2>
      <table id="popularTable">
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Price</th>
            <th>Change</th>
            <th>%</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody>
          <tr><td colspan="5">Loading…</td></tr>
        </tbody>
      </table>

      <h2>Search for a Stock</h2>
      <input id="symbolInput" placeholder="Enter symbol (e.g. AAPL)">
      <button id="searchBtn">Search</button>

      <table id="stockTable" class="hidden">
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Price</th>
            <th>Change</th>
            <th>%</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>
  </main>

  <script>
    async function post(action, data) {
      const res = await fetch(window.location.pathname, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action, ...data })
      });
      return res.json().catch(() => ({ status: 'error', message: 'Invalid response' }));
    }

    async function validate() {
      const key = sessionStorage.getItem('sessionKey');
      if (!key) {
        document.getElementById('statusText').textContent = 'No session found.';
        setTimeout(() => location.href = 'index.html', 700);
        return;
      }
      const data = await post('validate', { session: key });
      if (data.status === 'success') {
        document.getElementById('statusText').classList.add('hidden');
        document.getElementById('authedArea').classList.remove('hidden');
        const summary = await post('get_portfolio_summary', { session: key });
        if (summary.status === 'success') {
          document.getElementById('bpValue').textContent = '$' + Number(summary.buying_power).toFixed(2);
        }
        loadPopular();
      } else {
        document.getElementById('statusText').textContent = 'Session expired. Please login again.';
        setTimeout(() => location.href = 'index.html', 900);
      }
    }

    async function loadPopular() {
      const tbody = document.querySelector('#popularTable tbody');
      tbody.innerHTML = '<tr><td colspan="5">Loading…</td></tr>';
      const data = await post('fetch_popular', {});
      tbody.innerHTML = '';
      if (data.status === 'success' && Array.isArray(data.data)) {
        for (const s of data.data) {
          if (s && s.status === 'success') {
            tbody.innerHTML += `
              <tr>
                <td>${s.symbol}</td>
                <td>${s.price}</td>
                <td>${s.change}</td>
                <td>${s.percent}%</td>
                <td>
                  <button onclick="buyStock('${s.symbol}', ${s.price})">Buy</button>
                  <button onclick="placeBuyLimit('${s.symbol}', ${s.price})">Buy Limit</button>
                </td>
              </tr>`;
          }
        }
      } else {
        tbody.innerHTML = '<tr><td colspan="5">Error loading popular stocks</td></tr>';
      }
    }

    function buyStock(symbol, price) {
      const input = prompt('Buy ' + symbol + '\nEnter number of shares to buy (whole shares only):');
      if (input === null) return;
      const requestedShares = parseInt(input, 10);
      if (!Number.isFinite(requestedShares) || requestedShares <= 0) {
        alert('Please enter a valid number of whole shares.');
        return;
      }
      const key = sessionStorage.getItem('sessionKey');
      if (!key) {
        alert('Session expired. Please log in again.');
        location.href = 'index.html';
        return;
      }
      const requestedPrice = Number(price);
      const payload = { action: 'buy_stock', session: key, symbol, shares: requestedShares, price: requestedPrice };

      fetch(window.location.pathname, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      })
      .then(async (res) => {
        const raw = await res.text();
        let data = null;
        try {
          data = JSON.parse(raw);
          if (typeof data === 'string') {
            data = JSON.parse(data);
          }
        } catch (e) {}

        const envelopeOk = data && data.status === 'success';
        const inner = data && data.data ? data.data : data || {};
        const innerOk = inner && inner.status === 'success';
        const ok = envelopeOk || innerOk;

        if (ok) {
          const qty  = Number(inner.quantity ?? inner.shares ?? inner.qty ?? requestedShares);
          const px   = Number(inner.executed_price ?? inner.fill_price ?? inner.price ?? requestedPrice);
          const sym  = inner.symbol || symbol;

          alert('Trade executed successfully!\nBought ' + qty + ' ' + sym + ' at $' + px.toFixed(2));

          const bp = inner.buying_power ?? data.buying_power;
          if (bp !== undefined && bp !== null) {
            document.getElementById('bpValue').textContent = '$' + Number(bp).toFixed(2);
          }
        } else {
          const msg = (data && (data.message || (data.data && data.data.message))) || 'Unexpected error.';
          alert('Trade failed: ' + msg);
        }
      })
      .catch(() => {
        alert('The trade may have completed, but the response timed out. Please refresh your portfolio.');
      });
    }

    async function placeBuyLimit(symbol, currentPrice) {
      const key = sessionStorage.getItem('sessionKey');
      if (!key) {
        alert('Session expired. Please log in again.');
        location.href = 'index.html';
        return;
      }
      const qtyInput = prompt('Enter number of shares to BUY for ' + symbol + ' (whole shares only):');
      const qty = parseFloat(qtyInput);
      if (!isFinite(qty) || qty <= 0) {
        alert('Invalid quantity.');
        return;
      }
      const limitInput = prompt('Enter your BUY LIMIT price for ' + symbol + ':\n(Current price: $' + currentPrice + ')');
      const limitPrice = parseFloat(limitInput);
      if (!isFinite(limitPrice) || limitPrice <= 0) {
        alert('Invalid limit price.');
        return;
      }
      const res = await post('request_buy_limit_trade', {
        session: key,
        symbol,
        quantity: qty,
        side: 'buy',
        limit_price: limitPrice
      });
      if (res.status === 'success') {
        alert('Buy limit order placed for ' + qty + ' ' + symbol + ' @ $' + limitPrice);
      } else {
        alert(res.message || 'Failed to place buy limit order.');
      }
    }

    document.getElementById('logoutBtn').onclick = () => {
      sessionStorage.removeItem('sessionKey');
      location.href = 'index.html';
    };
    document.getElementById('portfolioBtn').onclick = () => {
      location.href = 'portfolio.php';
    };
    document.getElementById('depositBtn').onclick = async () => {
      const amt = parseFloat(prompt('Enter deposit amount:'));
      if (!isFinite(amt) || amt <= 0) return alert('Invalid amount.');
      const key = sessionStorage.getItem('sessionKey');
      const res = await post('deposit', { session: key, amount: amt });
      if (res.status === 'success') {
        alert('Deposit successful: $' + amt.toFixed(2));
        document.getElementById('bpValue').textContent = '$' + Number(res.buying_power).toFixed(2);
      } else {
        alert(res.message || 'Deposit failed.');
      }
    };
    document.getElementById('searchBtn').onclick = async () => {
      const sym = document.getElementById('symbolInput').value.trim();
      if (!sym) return alert('Enter a symbol.');
      const data = await post('fetch_stock', { symbol: sym });
      const tbody = document.querySelector('#stockTable tbody');
      const table = document.getElementById('stockTable');
      tbody.innerHTML = '';
      if (data.status === 'success' && data.symbol) {
        table.classList.remove('hidden');
        tbody.innerHTML = `
          <tr>
            <td>${data.symbol}</td>
            <td>${data.price}</td>
            <td>${data.change}</td>
            <td>${data.percent}%</td>
            <td>
              <button onclick="buyStock('${data.symbol}', ${data.price})">Buy</button>
              <button onclick="placeBuyLimit('${data.symbol}', ${data.price})">Buy Limit</button>
            </td>
          </tr>`;
      } else {
        table.classList.remove('hidden');
        tbody.innerHTML = '<tr><td colspan="5">' + (data.message || 'Error fetching stock') + '</td></tr>';
      }
    };
    document.addEventListener('visibilitychange', async () => {
      if (document.visibilityState === 'visible') {
        const key = sessionStorage.getItem('sessionKey');
        if (!key) return;
        const summary = await post('get_portfolio_summary', { session: key });
        if (summary.status === 'success') {
          document.getElementById('bpValue').textContent = '$' + Number(summary.buying_power).toFixed(2);
        }
      }
    });
    validate();
  </script>
</body>
</html>

