<?php
require __DIR__ . '/vendor/autoload.php';


use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;


$configPath = __DIR__ . '/deploy_config.ini';
$config = parse_ini_file($configPath);
if ($config === false) {
   echo "[X] Could not read the config file {$configPath}\n";
}


$config['vhost'] = 'deployVhost';


$needRabbit = ['rabbitHost', 'rabbitUser', 'rabbitPass', 'vhost'];
foreach ($needRabbit as $k) {
   if (empty($config[$k])) {
       echo "[X] Missing '{$k}' in {$configPath} (RabbitMQ config)\n";
   }
}


$needMysql = ['mysqlHost', 'mysqlUser', 'mysqlPass', 'mysqlDb'];
foreach ($needMysql as $k) {
   if (empty($config[$k])) {
       echo "[X] Missing '{$k}' in {$configPath} (MySQL config)\n";
   }
}








$gitDir     = '/home/latchman/git/it490-rabbitmqphp';
$stageDir   = __DIR__ . '/source';
$bundleName = 'db_backend';
$target     = 'qa';
$component  = 'database';;
$archiveLocal = "/home/latchman/opt/deploy/archive/db_backend.zip";




function run_or_fail($cmd) {
   echo "[CMD] $cmd\n";
   $rc = 0;
   system($cmd . ' 2>&1', $rc);
   if ($rc !== 0) {
       echo "[X] Command failed (rc=$rc): $cmd\n";
   }
}






function ensure_dir($path) {
   if (!is_dir($path)) {
       echo "[X] Can't find directory: $path\n";
   }
}






ensure_dir($gitDir);
run_or_fail("rm -rf " . escapeshellarg($stageDir));
run_or_fail("mkdir -p " . escapeshellarg($stageDir));
run_or_fail("cp -r " . escapeshellarg($gitDir) . " " . escapeshellarg($stageDir . '/app'));


$configDir = $stageDir . '/app/config';
run_or_fail("mkdir -p " . escapeshellarg($configDir));
if (!copy($configPath, $configDir . '/deploy_config.ini')) {
   echo "[X] Failed to copy deploy_config.ini into bundle.\n";
}






$rabbitCfgDir = $configDir . '/rabbitmq';
run_or_fail("mkdir -p " . escapeshellarg($rabbitCfgDir));


$host =  'localhost';
$port = 15672;
$user = 'latch';
$password = 'latch';
$vhost = urlencode('projectVhost');


$url = "http://$host:$port/api/definitions/$vhost";


$ch = curl_init();
curl_setopt($ch, CURLOPT_URL, $url);
curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
curl_setopt($ch, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
curl_setopt($ch, CURLOPT_USERPWD, "$user:$password");
curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);




$response = curl_exec($ch);






if (curl_errno($ch)) {
   echo "RabbitMQ Export Error: " . curl_error($ch) . "\n";
   curl_close($ch);
   exit;
}




curl_close($ch);


$definitions = json_decode($response, true);






if (!$definitions) {
   echo "[X] Failed to decode RabbitMQ JSON\n";
   exit;
}






echo "Exchanges found: " . count($definitions['exchanges']) . "\n";
echo "Queues found: " . count($definitions['queues']) . "\n";
echo "Bindings found: " . count($definitions['bindings']) . "\n";






$definitionsFile = $rabbitCfgDir . "/rabbit_definitions_projectVhost.json";
file_put_contents($definitionsFile, json_encode($definitions, JSON_PRETTY_PRINT));




echo "[*] Saved definitions to $definitionsFile\n";






$dbDir = $stageDir . '/db';
run_or_fail("mkdir -p " . escapeshellarg($dbDir));






$mysqlHost = $config['mysqlHost'];
$mysqlUser = $config['mysqlUser'];
$mysqlPass = $config['mysqlPass'];
$mysqlDb   = $config['mysqlDb'];
$dumpFile  = $dbDir . '/mysql_dump.sql';






$mysqldumpCmd = sprintf(
   "MYSQL_PWD=%s mysqldump -h%s -u%s %s > %s",
   escapeshellarg($mysqlPass),
   escapeshellarg($mysqlHost),
   escapeshellarg($mysqlUser),
   escapeshellarg($mysqlDb),
   escapeshellarg($dumpFile)
);




run_or_fail($mysqldumpCmd);


if (!file_exists($dumpFile) || filesize($dumpFile) === 0) {
   echo "[X] mysqldump failed or created an empty dump: {$dumpFile}\n";
}






$archiveDir = dirname($archiveLocal);
if (!is_dir($archiveDir)) {
   run_or_fail("mkdir -p " . escapeshellarg($archiveDir));
}




@unlink($archiveLocal);






run_or_fail(sprintf(
   'cd %s && zip -qry %s app db config -x "*.git/*" "*/.git/*" "*/vendor/*" "*.log" "*/cache/*" "*/node_modules/*"',
   escapeshellarg($stageDir),
   escapeshellarg($archiveLocal)
));




echo "[*] Bundle ready locally at {$archiveLocal}\n";




run_or_fail(sprintf(
   'unzip -l %s | grep mysql_dump.sql || echo "[WARN] mysql_dump.sql not found inside zip"',
   escapeshellarg($archiveLocal)
));






$remoteHost = $config['rabbitHost'];
$remoteUser = 'deploy';
$remoteDir  = '/home/deploy/opt/deploy/archive';
$deployPass = $config['deployPass'] ?? '';






if ($deployPass === '') {
   echo "[X] Missing deployPass in {$configPath}\n";
}




$pass  = escapeshellarg($deployPass);
$user  = escapeshellarg($remoteUser);
$host  = escapeshellarg($remoteHost);
$dir   = escapeshellarg($remoteDir);
$file  = escapeshellarg($archiveLocal);




echo "[*] Sending to deploy server: {$remoteHost}\n";




run_or_fail("sshpass -p $pass ssh -o StrictHostKeyChecking=no $user@$host mkdir -p $dir");
run_or_fail("sshpass -p $pass scp -o StrictHostKeyChecking=no $file $user@$host:$dir/");




$remoteArchive = "{$remoteDir}/" . basename($archiveLocal);
echo "[*] Sending archive to $remoteArchive\n";






$data = [
   'bundle_name'  => $bundleName,
   'target_env'   => $target,
   'component'    => $component,
   'archive_path' => $remoteArchive,
];






try {
   echo "[*] Connecting to RabbitMQ {$config['rabbitHost']}:5672 vhost '{$config['vhost']}'...\n";


   $conn = new AMQPStreamConnection(
       $config['rabbitHost'],
       5672,
       $config['rabbitUser'],
       $config['rabbitPass'],
       $config['vhost'],
       false,
       'AMQPLAIN',
       null,
       'en_US',
       30.0,
       30.0,
       null,
       false,
       30
   );




   $ch = $conn->channel();
   $ch->queue_declare('deploy_request', false, true, false, false);


   $payload = json_encode($data, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
   $msg = new AMQPMessage($payload, ['content_type' => 'application/json', 'delivery_mode' => 2]);


   $ch->basic_publish($msg, '', 'deploy_request');


   echo "[*] Sent database bundle '{$bundleName}' to 'deploy_request'\n";
   echo "       Payload: {$payload}\n";


   $ch->close();
   $conn->close();
} catch (Throwable $e) {
   echo "[*] RabbitMQ publish failed: " . $e->getMessage() . "\n";
}




echo "[*] Database bundle complete from this end, deploy listener can now pick it up.\n";
