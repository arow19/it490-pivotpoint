<?php
//deploy test #2
require_once __DIR__ . '/vendor/autoload.php';
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    header('Content-Type: application/json');

    $raw       = file_get_contents('php://input');
    $payload   = json_decode($raw, true) ?: [];
    $action    = $payload['action'] ?? '';
    $session   = $payload['session'] ?? '';
    $amount    = isset($payload['amount']) ? floatval($payload['amount']) : 0.0;
    $symbol    = $payload['symbol'] ?? '';
    $quantity  = isset($payload['quantity']) ? floatval($payload['quantity']) : 0.0;
    $side      = $payload['side'] ?? '';
    $price     = isset($payload['price']) ? floatval($payload['price']) : 0.0;
    $limitPrice= isset($payload['limit_price']) ? floatval($payload['limit_price']) : 0.0;
    $window    = isset($payload['window']) ? intval($payload['window']) : 0;

    if ($action === '') {
        http_response_code(400);
        echo json_encode(['status' => 'error', 'message' => 'Missing action']);
        exit;
    }
    if ($session === '') {
        http_response_code(401);
        echo json_encode(['status' => 'error', 'message' => 'Missing or invalid session']);
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

        $requestQueue  = 'portfolio_request';
        $responseQueue = 'portfolio_response';

        if ($action === 'request_sell_stock') {
            $requestQueue  = 'request_sell_stock';
            $responseQueue = 'response_sell_stock';
        } elseif ($action === 'request_trade_execution') {
            $requestQueue  = 'request_trade_execution';
            $responseQueue = 'response_trade_execution';
        } elseif ($action === 'get_volatility') {
            $requestQueue  = 'volatility_request';
            $responseQueue = 'volatility_response';
        } elseif ($action === 'request_sell_limit_trade') {
            $requestQueue  = 'request_sell_limit_trade';
            $responseQueue = 'response_sell_limit_trade';
        }

        $ch->queue_declare($requestQueue, false, true, false, false);
        $ch->queue_declare($responseQueue, false, true, false, false);

        $corrId  = bin2hex(random_bytes(12));
        $msgData = [
            'action'  => $action,
            'session' => $session
        ];
        if ($amount > 0)     $msgData['amount']      = $amount;
        if ($symbol !== '')  $msgData['symbol']      = $symbol;
        if ($quantity > 0)   $msgData['quantity']    = $quantity;
        if ($side !== '')    $msgData['side']        = $side;
        if ($price > 0)      $msgData['price']       = $price;
        if ($limitPrice > 0) $msgData['limit_price'] = $limitPrice;
        if ($window > 0)     $msgData['window']      = $window;

        $msg = new AMQPMessage(
            json_encode($msgData),
            [
                'correlation_id' => $corrId,
                'reply_to'       => $responseQueue,
                'content_type'   => 'application/json'
            ]
        );

        $ch->basic_publish($msg, '', $requestQueue);

        $response = null;
        $callback = function ($m) use (&$response, $corrId) {
            if ($m->get('correlation_id') === $corrId) $response = $m->body;
        };

        $ch->basic_consume($responseQueue, '', false, true, false, false, $callback);

        $start = time();
        while ($response === null && (time() - $start) < 6) {
            $ch->wait(null, false, 1);
        }

        $ch->close();
        $conn->close();

        if ($response === null) {
            http_response_code(504);
            echo json_encode(['status' => 'error', 'message' => 'No response from portfolio service']);
            exit;
        }

        $decoded = json_decode($response, true);
        if (json_last_error() === JSON_ERROR_NONE) {
            echo json_encode($decoded);
        } else {
            echo $response;
        }
        exit;

    } catch (Throwable $e) {
        http_response_code(500);
        echo json_encode(['status' => 'error', 'message' => 'Server error', 'detail' => $e->getMessage()]);
        exit;
    }
}
?>
<html>
<head>
  <title>Portfolio - Pivot Point</title>
  <style>
    body { font-family: Arial, sans-serif; background: #eef2f7; margin: 0; padding: 0; }
    header { background: #007bff; color: white; padding: 15px 20px; display: flex; justify-content: space-between; align-items: center; gap: 12px; flex-wrap: wrap; }
    .header-actions { display: flex; gap: 10px; align-items: center; }
    .pill { background: rgba(255,255,255,0.15); padding: 6px 10px; border-radius: 6px; font-weight: bold; }
    main { padding: 25px; max-width: 1000px; margin: 0 auto; }
    .grid { display: grid; grid-template-columns: 1fr; gap: 20px; }
    @media (min-width: 900px) { .grid { grid-template-columns: 1fr 1fr; } }
    .card { background: white; border-radius: 8px; padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
    .card h2 { margin-top: 0; }
    .row { display: flex; gap: 16px; flex-wrap: wrap; }
    .metric { background: #f7f9fc; border: 1px solid #e5eaf1; border-radius: 8px; padding: 12px 14px; min-width: 180px; }
    .metric .label { color: #666; font-size: 12px; }
    .metric .value { font-size: 18px; font-weight: bold; }
    table { width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
    th, td { padding: 10px; text-align: center; border-bottom: 1px solid #ddd; }
    th { background: #007bff; color: white; }
    .pl-pos { color: #0a8a2a; font-weight: bold; }
    .pl-neg { color: #b00020; font-weight: bold; }
    input { padding: 8px; border: 1px solid #ccc; border-radius: 4px; }
    button { padding: 8px 14px; background: #007bff; color: white; border: none; border-radius: 5px; cursor: pointer; }
    button:hover { background: #0056b3; }
    .muted { color: #666; }
  </style>
</head>
<body>
  <header>
    <h1 style="margin:0;">My Portfolio</h1>
    <div class="header-actions">
      <span class="pill">Buying Power: <span id="bpHeader">$0.00</span></span>
      <button id="refreshBtn">Refresh</button>
      <button id="homeBtn">Home</button>
      <button id="logoutBtn">Logout</button>
    </div>
  </header>

  <main>
    <div class="grid">
      <div class="card">
        <h2>Account Summary</h2>
        <div class="row">
          <div class="metric"><div class="label">Buying Power</div><div class="value" id="buyingPower">$0.00</div></div>
          <div class="metric"><div class="label">Total Market Value</div><div class="value" id="totalMV">$0.00</div></div>
          <div class="metric"><div class="label">Total Balance</div><div class="value" id="totalBal">$0.00</div></div>
          <div class="metric"><div class="label">Total P/L</div><div class="value" id="totalPL">$0.00 (0.00%)</div></div>
        </div>
        <p class="muted" id="summaryText" style="margin-top:10px;">Loading account data...</p>
      </div>

      <div class="card">
        <h2>Deposit Funds</h2>
        <div class="row">
          <input type="number" id="depositAmount" placeholder="Enter amount (e.g. 500)" step="0.01" min="0">
          <button id="depositBtn">Deposit</button>
        </div>
        <p id="depositMsg" class="muted" style="margin-top:8px;"></p>
      </div>
    </div>

    <div class="card" style="margin-top:20px;">
      <h2>Holdings</h2>
      <table id="holdingsTable">
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Quantity</th>
            <th>Avg Cost</th>
            <th>Current Price</th>
            <th>Total Market Value</th>
            <th>P/L</th>
            <th>P/L %</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody>
          <tr><td colspan="8">Loading holdings…</td></tr>
        </tbody>
      </table>
    </div>
  </main>

  <script>
    function fmt(n) { return '$' + (Number(n) || 0).toFixed(2) }
    function fmtPct(n) { return (Number(n) || 0).toFixed(2) + '%' }
    function plClass(n) { return Number(n) >= 0 ? 'pl-pos' : 'pl-neg' }

    let gBuyingPower = 0;

    async function post(action, data = {}) {
      const res = await fetch(window.location.pathname, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action, ...data })
      });
      return res.json().catch(() => ({ status: 'error', message: 'Invalid JSON' }));
    }

    async function sellStock(symbol, quantity) {
      const key = sessionStorage.getItem('sessionKey');
      const input = prompt('You own ' + quantity + ' shares of ' + symbol + '.\nHow many would you like to sell?', quantity);
      const sellQty = parseFloat(input);
      if (!isFinite(sellQty) || sellQty <= 0) { alert('Invalid quantity entered.'); return; }
      if (sellQty > quantity) { alert('You cannot sell more shares than you own.'); return; }

      const priceRes = await post('request_sell_stock', { session: key, symbol });
      if (priceRes.status !== 'success' || !priceRes.price) { alert(priceRes.message || 'Failed to fetch live stock price.'); return; }
      const livePrice = Number(priceRes.price);
      const ok = confirm('Live price for ' + symbol + ': ' + fmt(livePrice) + '\n\nSell ' + sellQty + ' shares now?');
      if (!ok) return;

      const res = await post('request_trade_execution', { session: key, symbol, quantity: sellQty, side: 'sell', price: livePrice });
      if (res.status === 'success') {
        alert('Sold ' + sellQty + ' shares of ' + symbol + ' at ' + fmt(livePrice) + ' successfully.');
        await loadSummary();
        await loadHoldings();
      } else {
        alert(res.message || 'Sell failed.');
      }
    }

    async function placeSellLimit(symbol, ownedQty) {
      const key = sessionStorage.getItem('sessionKey');
      const qtyInput = prompt('You own ' + ownedQty + ' shares of ' + symbol + '.\nHow many shares would you like to SELL via LIMIT order?', ownedQty);
      const qty = parseFloat(qtyInput);
      if (!isFinite(qty) || qty <= 0 || qty > ownedQty) { alert('Invalid quantity entered.'); return; }

      const limitInput = prompt('Enter your SELL LIMIT price for ' + symbol + ':');
      const limitPrice = parseFloat(limitInput);
      if (!isFinite(limitPrice) || limitPrice <= 0) { alert('Invalid limit price entered.'); return; }

      const res = await post('request_sell_limit_trade', { session: key, symbol, quantity: qty, side: 'sell', limit_price: limitPrice });
      if (res.status === 'success') {
        alert('Sell limit order placed for ' + qty + ' ' + symbol + ' @ $' + limitPrice);
      } else {
        alert(res.message || 'Failed to place sell limit order.');
      }
    }

    async function showVolatility(symbol) {
      const key  = sessionStorage.getItem('sessionKey');
      const days = 60;

      try {
        const res = await post('get_volatility', { session: key, symbol, days });
        const payload = (res && res.data) ? res.data : res;

        if (payload && typeof payload.daily_vol !== 'undefined' && typeof payload.annual_vol !== 'undefined') {
          const dailyPct  = (Number(payload.daily_vol)  * 100).toFixed(3);
          const annualPct = (Number(payload.annual_vol) * 100).toFixed(2);
          const asOf = payload.to || payload.timestamp || 'latest';
          const sym = payload.symbol || symbol;

          let recommendation = 'Hold';
          const annualVol = Number(payload.annual_vol);
          if (annualVol >= 0.60) {
            recommendation = 'Consider selling or reducing position (high volatility).';
          } else if (annualVol >= 0.35) {
            recommendation = 'Hold or size carefully (moderate volatility).';
          } else {
            recommendation = 'Consider buying or adding (low volatility).';
          }

          alert(
            'Volatility for ' + sym + ' (as of ' + asOf + ')\n' +
            'Window: ' + (payload.window_days || days) + ' trading days\n' +
            'Daily: ' + dailyPct + '%\n' +
            'Annualized: ' + annualPct + '%\n\n' +
            'Recommendation: ' + recommendation
          );
        } else {
          const msg = (res && res.message) ? res.message : 'Failed to get volatility.';
          alert(msg);
        }
      } catch (err) {
        alert('Error fetching volatility.');
      }
    }

    async function loadSummary() {
      const key = sessionStorage.getItem('sessionKey');
      const data = await post('get_portfolio_summary', { session: key });
      if (data.status === 'success') {
        gBuyingPower = Number(data.buying_power || 0);
        document.getElementById('buyingPower').textContent = fmt(gBuyingPower);
        document.getElementById('bpHeader').textContent = fmt(gBuyingPower);

        if (data.total_market_value != null) {
          document.getElementById('totalMV').textContent = fmt(data.total_market_value);
        }
        if (data.total_pl != null && data.total_pl_percent != null) {
          document.getElementById('totalPL').textContent = fmt(data.total_pl) + ' (' + fmtPct(data.total_pl_percent) + ')';
        }

        const mvVisible = parseFloat((document.getElementById('totalMV').textContent || '$0').replace(/[^0-9.-]/g, '')) || 0;
        document.getElementById('totalBal').textContent = fmt(gBuyingPower + mvVisible);
        document.getElementById('summaryText').textContent = 'Portfolio loaded.';
      } else {
        document.getElementById('summaryText').textContent = data.message || 'Error loading portfolio.';
      }
    }

    async function loadHoldings() {
      const key = sessionStorage.getItem('sessionKey');
      const data = await post('get_holdings', { session: key });
      const tbody = document.querySelector('#holdingsTable tbody');
      tbody.innerHTML = '';

      if (data.status === 'success' && Array.isArray(data.holdings)) {
        let totalMV = 0, totalPL = 0, totalCost = 0;

        for (const h of data.holdings) {
          const qty = Number(h.quantity || 0);
          const avg = Number(h.price || 0);
          const rowId = 'row-' + h.symbol;

          tbody.innerHTML += `
            <tr id="${rowId}">
              <td>${h.symbol ?? '-'}</td>
              <td>${qty}</td>
              <td>${fmt(avg)}</td>
              <td id="price-${h.symbol}">Loading...</td>
              <td id="mv-${h.symbol}">...</td>
              <td id="pl-${h.symbol}">...</td>
              <td id="plp-${h.symbol}">...</td>
              <td>${qty > 0 ? `<button onclick="sellStock('${h.symbol}', ${qty})">Sell</button>
                <button onclick="placeSellLimit('${h.symbol}', ${qty})">Sell Limit</button>
                <button onclick="showVolatility('${h.symbol}')">Volatility</button>` : '-'}</td>
            </tr>`;
        }

        for (const h of data.holdings) {
          const symbol = h.symbol;
          const qty    = Number(h.quantity || 0);
          const avg    = Number(h.price || 0);

          const liveRes   = await post('request_sell_stock', { session: key, symbol });
          const livePrice = (liveRes && liveRes.price) ? Number(liveRes.price) : avg;

          const mv   = qty * livePrice;
          const pl   = mv - qty * avg;
          const cost = qty * avg;
          const plp  = cost ? (pl / cost) * 100 : 0;

          document.getElementById('price-' + symbol).textContent = fmt(livePrice);
          document.getElementById('mv-' + symbol).textContent    = fmt(mv);
          document.getElementById('pl-' + symbol).textContent    = fmt(pl);
          document.getElementById('pl-' + symbol).className      = plClass(pl);
          document.getElementById('plp-' + symbol).textContent   = fmtPct(plp);
          document.getElementById('plp-' + symbol).className     = plClass(pl);

          totalMV  += mv;
          totalPL  += pl;
          totalCost+= cost;
        }

        const plPercent = totalCost ? (totalPL / totalCost) * 100 : 0;
        document.getElementById('totalMV').textContent = fmt(totalMV);
        document.getElementById('totalPL').textContent = fmt(totalPL) + ' (' + fmtPct(plPercent) + ')';
        document.getElementById('totalBal').textContent = fmt(gBuyingPower + totalMV);
      } else {
        tbody.innerHTML = `<tr><td colspan="8">${data.message || 'No holdings found.'}</td></tr>`;
        document.getElementById('totalMV').textContent = fmt(0);
        document.getElementById('totalPL').textContent = fmt(0) + ' (' + fmtPct(0) + ')';
        document.getElementById('totalBal').textContent = fmt(gBuyingPower);
      }
    }

    async function depositFunds() {
      const key = sessionStorage.getItem('sessionKey');
      const amt = parseFloat(document.getElementById('depositAmount').value);
      const msg = document.getElementById('depositMsg');
      msg.textContent = '';

      if (!isFinite(amt) || amt <= 0) {
        msg.textContent = 'Enter a valid amount.';
        return;
      }

      const res = await post('deposit', { session: key, amount: amt });
      if (res.status === 'success') {
        document.getElementById('depositAmount').value = '';
        msg.textContent = 'Deposit successful.';
        await loadSummary();
      } else {
        msg.textContent = res.message || 'Deposit failed.';
      }
    }

    document.getElementById('depositBtn').onclick = depositFunds;
    document.getElementById('homeBtn').onclick    = () => location.href = 'home.php';
    document.getElementById('logoutBtn').onclick  = () => { sessionStorage.removeItem('sessionKey'); location.href = 'index.html'; };
    document.getElementById('refreshBtn').onclick = () => { loadSummary(); loadHoldings(); };

    (async () => { await loadSummary(); await loadHoldings(); })();
  </script>
</body>
</html>

