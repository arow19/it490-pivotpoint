<?php
require __DIR__ . '/vendor/autoload.php';
use PhpAmqpLib\Connection\AMQPStreamConnection;
$conf = parse_ini_file(__DIR__ . '/deploy_config.ini', true);
$host = parse_ini_file(__DIR__ . '/deploy_hosts.ini', true);

if (!$conf || !$host) 
    {
         echo "missing config(s)\n"; exit(1); 
    }

$db = new mysqli($conf['mysql']['dbHost'], $conf['mysql']['dbUser'], $conf['mysql']['dbPass'], $conf['mysql']['dbName']);

if ($db->connect_error)
{
    echo "db connect failed: ";
    exit(1);
}
try 
{
    $connection = new AMQPStreamConnection($conf['rabbitmq']['rabbitHost'], 5672, $conf['rabbitmq']['rabbitUser'], $conf['rabbitmq']['rabbitPass'], $conf['rabbitmq']['vhost'], false, 'AMQPLAIN', null, 'en_US', 60.0, 180.0, null, false, 0);
    $channel = $connection->channel();
    $channel->queue_declare('prod_request', false, true, false, false);
    echo "listener ready\n";
    } catch (Throwable $exception) {
echo "rabbitmq connect failed ".$exception->getMessage()."\n";
exit(1);
}

    function runBash(string $bashCommand): int
        {
        $exitCode = 0;
        $maxRetries = 100;
        $sleepSeconds = 10;

for ($attempt = 1; $attempt <= $maxRetries; $attempt++) 
    {
        exec($bashCommand . " >/dev/null 2>&1", $output, $exitCode);
        if ($exitCode === 0) 
    {
        return 0;
    }
    sleep($sleepSeconds);
    }
        return $exitCode;
        }

function againCodes(string $action, int $max = 5, int $sleep = 10)
    {
    for ($i = 1; $i <= $max; $i++) 
        {
            if (runBash($action) === 0) 
                {
                    return 0;
                }
        sleep($sleep);
        }
    return 1;
    }

function logOutput(string $shellCommand)
    {
        echo "cmd: {$shellCommand}\n";
        $lines = [];
        $exitCode = 0;
    exec($shellCommand, $lines, $exitCode);
    foreach ($lines as $line) 
        {
    echo strtolower($line) . "\n";
        }
        if ($exitCode !== 0) 
        {
    echo "command failed ({$exitCode})\n";
        }
   return $exitCode;
        }
    function goAgain(string $action, int $max = 5, int $sleep = 10)
        {
    for ($i = 1; $i <= $max; $i++) 
        {
    echo "retry attempt {$i}/{$max}\n";
    $result = runBash($action);
    if ($result === 0) 
        {
    return 0;
        }
    if ($i < $max) {
    echo "retry sleeping {$sleep}s\n";
    sleep($sleep);
        }
            }
        return 1;
    }

function silentConnect(array $vm, string $remote, int $deadline = 900) 
    {
        $password = escapeshellarg($vm['pass']);
        $user = escapeshellarg($vm['user']);
        $host = escapeshellarg($vm['host']);
        $command = escapeshellarg($remote);
        $options = "-o StrictHostKeyChecking=no -o ConnectTimeout=60 -o ServerAliveInterval=30 -o ServerAliveCountMax=20 -o TCPKeepAlive=yes -o PreferredAuthentications=password -o PubkeyAuthentication=no";
        $sshCommand = "timeout {$deadline} sshpass -p {$password} ssh {$options} {$user}@{$host} {$command}";
        return $sshCommand;
    }

function secureCopy(array $vm, string $local, string $remoteDir, int $deadline = 900)
    {

    $password = escapeshellarg($vm['pass']);
    $user = escapeshellarg($vm['user']);
    $host = escapeshellarg($vm['host']);
    $file = escapeshellarg($local);
    $remotePath = escapeshellarg(rtrim($remoteDir, '/') . '/');
    $options = "-o StrictHostKeyChecking=no -o ConnectTimeout=60 -o ServerAliveInterval=30 -o ServerAliveCountMax=20 -o TCPKeepAlive=yes -o PreferredAuthentications=password -o PubkeyAuthentication=no";
    $scpCommand = "timeout {$deadline} sshpass -p {$password} scp {$options} {$file} {$user}@{$host}:{$remotePath}";
        return $scpCommand;
    }


        function goodZip(string $file) 
        {
    if (!file_exists($file))
    {
        return false;
    }
    $zip = new ZipArchive();

    if ($zip->open($file) !== true)
    {
    return false;
    }
    $zip->close();
    return true;
    }
    function runRemote(array $vm, string $remoteCmd): int
{
$sshCommand = silentConnect($vm, $remoteCmd);
return runBash($sshCommand);
}
    function runRemoteRetry(array $vm, string $remoteCmd, int $max = 5, int $sleep = 10): int
        {
        for ($i = 1; $i <= $max; $i++) 
            {
    $result = runRemote($vm, $remoteCmd);
    if ($result === 0) 
        {
    return 0;
    }
        echo "remote retry attempt {$i}/{$max} failed, sleeping {$sleep}s\n";
        sleep($sleep);
    }
return 1;
}

    function lastVersion(mysqli $db, string $bundle)
        {
        $res = $db->query("select version from bundles where bundle_name='".$db->real_escape_string($bundle)."' and status='success' order by version desc limit 1");
            if (!$res)
            {
                return 0;
            }
        $row = $res->fetch_assoc();
        return $row ? (int)$row['version'] : 0;
        }

    function chooseVersion(mysqli $db, string $bundleName, string $label)
    {
        $sql = "select version, saved_path from bundles where bundle_name='".$db->real_escape_string($bundleName)."' and status='success' order by version desc";
        $res = $db->query($sql);
    if (!$res || $res->num_rows === 0)
    {
        echo "$label no successful versions found\n";
        return null;
    }
    $rows = [];
    echo "available $label versions\n";
    $i = 0;
    while ($row = $res->fetch_assoc())
    {
        $rows[] = $row;
        echo "[$i] {$row['version']}\n";
        $i++;
    }
     echo "enter the number of the $label version to deploy ";
    $choice = trim(fgets(STDIN));
    if ($choice === '' || !ctype_digit($choice))
    {
        echo "$label invalid selection\n";
        return null;
    }
    $versionID = (int)$choice;
    if (!isset($rows[$versionID]))
    {
    echo "$label invalid selection\n";
    return null;
     }
    return (int)$rows[$versionID]['version'];
    }
    function listVersionsFromDB(mysqli $db, string $bundle)
    {
        $sql = "select version from bundles where bundle_name='".$db->real_escape_string($bundle)."' and status='success' order by version desc";
        $res = $db->query($sql);
    if (!$res || $res->num_rows === 0)
    {
    return [];
        }
    $versions = [];
    while ($row = $res->fetch_assoc())
    {
        $versions[] = $row['version'];
    }
 return $versions;
    }

    function deployDmzVersion(array $dmz, int $dmzVer): string
    {
    $status = "success";
    $archive = "/home/deploy/opt/deploy/archive/dmz_bundle_v{$dmzVer}.zip";
     echo "dmz got bundle (version $dmzVer)\n";
    if (!goodZip($archive)) 
    {
    echo "dmz bad bundle\n";
    return "error";
    }
    echo "dmz backing up current version\n";
    $backupDir = "/home/andrew/backups/dmz_v{$dmzVer}";
    $backupDmzCmd = "bash -lc 'mkdir -p $backupDir && cp -rT {$dmz['deploy_root']} $backupDir'";
    $backupDmzSSH = silentConnect($dmz, $backupDmzCmd);
    runRemoteRetry($dmz, $backupDmzSSH, 100, 10);

    echo "dmz uploading\n";
    $uploadDmzCmd = secureCopy($dmz, $archive, $dmz['deploy_root']);
    if (runBash($uploadDmzCmd) !== 0)
    {
    return "error";
    }


    echo "dmz extracting\n";
    $extractDmzBundle = "bash -lc 'cd " . escapeshellarg($dmz['deploy_root']) . " && unzip -o " . escapeshellarg(basename($archive)) . " && rm -f " . escapeshellarg(basename($archive)) . "'";
    $extractDmzBundleSSH = silentConnect($dmz, $extractDmzBundle);
        if (runRemoteRetry($dmz, $extractDmzBundleSSH, 100, 10) !== 0)
        {
        return "error";
        }

    echo "dmz copying stock\n";
    $copyDmzStockCmd = "bash -lc \"cp -r '{$dmz['deploy_stock']}/.' '{$dmz['target_stock']}/'\"";
     $copyDmzStockSSH = silentConnect($dmz, $copyDmzStockCmd);
    if (runRemoteRetry($dmz, $copyDmzStockSSH, 100, 10) !== 0)
    {
        return "error";
    }
    echo "dmz copying emails\n";
    $copyDmzEmailsCmd = "bash -lc \"cp -r '{$dmz['deploy_emails']}/.' '{$dmz['target_emails']}/'\"";
    $copyDmzEmailsSSH = silentConnect($dmz, $copyDmzEmailsCmd);
    if (runRemoteRetry($dmz, $copyDmzEmailsSSH, 100, 10) !== 0)
    {
    return "error";
    }
 echo "dmz done\n";
 return "success";
    }   

    function deployFrontendVersion(array $front, int $frontVer): string
    {
        $status = "success";
        $archive = "/home/deploy/opt/deploy/archive/frontend_webapp_v{$frontVer}.zip";
        echo "frontend got bundle (version {$frontVer})\n";
            if (!goodZip($archive)) 
                {
                    echo "frontend bad bundle\n";
                    return "error";
                }

    echo "frontend making dirs\n";
    $makeFrontDirs = "bash -lc 'mkdir -p {$front['stage_dir']} {$front['target_dir']}'";
    if (runRemoteRetry($front, $makeFrontDirs, 100, 10) !== 0) 
        {
    echo "frontend failed to make dirs\n";
    return "error";
        }
    echo "frontend uploading\n";
        $uploadFrontBundle = secureCopy($front, $archive, $front['stage_dir']);
        if (runBash($uploadFrontBundle) !== 0) 
            {
        echo "frontend upload failed\n";
        return "error";
            }
        echo "frontend extracting\n";
        $extractFrontBundle = "bash -lc 'cd {$front['stage_dir']} && unzip -o frontend_webapp_v{$frontVer}.zip'";
        if (runRemoteRetry($front, $extractFrontBundle, 100, 10) !== 0)
             {
        echo "frontend extract failed\n";
        return "error";
    }

    echo "frontend copying files\n";
    $copyFrontFiles = "bash -lc 'echo " . escapeshellarg($front['pass']) . " | sudo -S cp -rT {$front['stage_dir']} {$front['target_dir']} >/dev/null 2>&1'";
    if (runRemoteRetry($front, $copyFrontFiles, 100, 10) !== 0) 
        {
    echo "frontend copy failed\n";
    return "error";
    }
        echo "frontend restarting services\n";
        $restartFrontService = "bash -lc 'echo " . escapeshellarg($front['pass']) . " | sudo -S {$front['restart']}'";
    if (runRemoteRetry($front, $restartFrontService, 100, 10) !== 0) 
        {
    echo "frontend restart failed\n";
    return "error";
        }
    echo "frontend done\n";
    return "success";
        }

    function deployDbmqVersion(array $dbmq, int $dbVer): string
    {
    $status = "success";
    $archive = "/home/deploy/opt/deploy/archive/db_backend_v{$dbVer}.zip";

    echo "database/rabbitmq got bundle (version $dbVer)\n";

    if (!goodZip($archive)) 
        {
        echo "database/rabbitmq bad or missing bundle $archive\n";
        return "error";
    }

    echo "database/rabbitmq making dirs\n";
    $makeDbDirsCmd = "bash -lc 'mkdir -p {$dbmq['deploy_dir']} {$dbmq['target_dir']}'";
    $makeDbDirsSSH = silentConnect($dbmq, $makeDbDirsCmd);
    if (runRemoteRetry($dbmq, $makeDbDirsSSH, 100, 10) !== 0) 
        {
        echo "database/rabbitmq failed to make dirs\n";
        return "error";
    }

    $cleanDbRootCmd = "bash -lc 'rm -rf {$dbmq['deploy_dir']}/*'";
    $cleanDbRootSSH = silentConnect($dbmq, $cleanDbRootCmd);
    if (runRemoteRetry($dbmq, $cleanDbRootSSH, 100, 10) !== 0) 
        {
        echo "database/rabbitmq failed to clean deploy_root\n";
        return "error";
    }

    echo "database/rabbitmq uploading\n";
    $uploadDbBundle = secureCopy($dbmq, $archive, $dbmq['deploy_dir']);
    if (runBash($uploadDbBundle) !== 0)
         {
        echo "database/rabbitmq upload failed\n";
        return "error";
    }

    echo "database/rabbitmq extracting\n";
    $extractDbBundleCmd = "bash -lc 'cd {$dbmq['deploy_dir']} && unzip -o " . escapeshellarg(basename($archive)) . " && rm -f " . escapeshellarg(basename($archive)) . "'";
    $extractDbBundleSSH = silentConnect($dbmq, $extractDbBundleCmd);
    if (runRemoteRetry($dbmq, $extractDbBundleSSH, 100, 10) !== 0) 
        {
        echo "database/rabbitmq extract failed\n";
        return "error";
    }

    echo "database/rabbitmq copying app\n";
    $copyDbAppCmd = "bash -lc \"echo " . escapeshellarg($dbmq['pass']) . " | sudo -S cp -r '{$dbmq['deploy_dir']}/app/.' '{$dbmq['target_dir']}/'\"";
    $copyDbAppSSH = silentConnect($dbmq, $copyDbAppCmd);
    if (runRemoteRetry($dbmq, $copyDbAppSSH, 100, 10) !== 0) 
        {
        echo "database/rabbitmq copy app failed\n";
        return "error";
    }

    echo "database/rabbitmq importing mysql\n";
    $importMySql = "bash -lc 'ROOT={$dbmq["deploy_dir"]}; DUMP=\$(find \"\$ROOT\" -name mysql_dump.sql | head -n1); if [ -z \"\$DUMP\" ]; then exit 1; fi; mysql -u {$dbmq["mysql_user"]} -p{$dbmq["mysql_pass"]} {$dbmq["mysql_db"]} < \"\$DUMP\"'";
    if (runRemoteRetry($dbmq, $importMySql, 100, 10) !== 0) 
        {
        echo "database/rabbitmq mysql import failed\n";
        return "error";
    }

    echo "database/rabbitmq importing rabbitmq definitions\n";
    $deployRoot = $dbmq['deploy_dir'];
    $importRabbitMQ = "bash -lc 'FILE=\$(find \"$deployRoot\" -type f -name rabbit_definitions_projectVhost.json | head -n1); if [ -z \"\$FILE\" ]; then echo \"no rabbit_definitions_projectVhost.json\"; exit 1; fi; curl -sS -u latch:latch -X POST -H \"Content-Type: application/json\" --data @\"\$FILE\" http://localhost:15672/api/definitions/projectVhost'";

    if (runRemoteRetry($dbmq, $importRabbitMQ, 100, 10) !== 0) 
        {
        echo "database/rabbitmq rabbit import failed\n";
        return "error";
    }

    echo "database/rabbitmq restarting services\n";
    $restartDbServiceCmd = "bash -lc \"echo " . escapeshellarg($dbmq['pass']) . " | sudo -S {$dbmq['restart']}\"";
    $restartDbServiceSSH = silentConnect($dbmq, $restartDbServiceCmd);
    if (runRemoteRetry($dbmq, $restartDbServiceSSH, 100, 10) !== 0) 
        {
        echo "database/rabbitmq restart failed\n";
        return "error";
    }

    echo "database/rabbitmq done version $dbVer\n";
    return "success";
    }

    function performRollback(array $host)
    {
        global $db;
        echo "rollback start\n";
        $frontQA = $host['frontend'];
        $dmzQA = $host['dmz'];
        $dbmqQA = $host['database'];
        $frontProd = $host['prod frontend'];
        $dmzProd = $host['prod dmz'];
        $dbmqProd = $host['prod database'];
         echo "available frontend versions\n";
        $versions = listVersionsFromDB($db, 'frontend_webapp');
        if (empty($versions))
         {
        echo "frontend no successful versions found\n";
         }
         else
         {
         foreach ($versions as $i => $v)
         {
         echo "[$i] version $v\n";
         }
     echo "enter the number of the frontend version to roll back to ";
    $choice = trim(fgets(STDIN));
     if (ctype_digit($choice) && isset($versions[$choice]))
         {
         $version = $versions[$choice];
        deployFrontendVersion($frontQA, $version);
         deployFrontendVersion($frontProd, $version);
         }
    else
    {
        echo "frontend invalid selection\n";
    }
        }

        echo "available dmz versions\n";
        $versions = listVersionsFromDB($db, 'dmz_bundle');
        if (empty($versions))
        {
        echo "dmz no successful versions found\n";
        }
        else
        {
        foreach ($versions as $i => $v)
        {
        echo "[$i] version $v\n";
        }
        echo "enter the number of the dmz version to roll back to ";
        $choice = trim(fgets(STDIN));
    if (ctype_digit($choice) && isset($versions[$choice]))
    {
        $version = $versions[$choice];
        deployDmzVersion($dmzQA, $version);
        deployDmzVersion($dmzProd, $version);
        }
        else
         {
         echo "dmz invalid selection\n";
        }
         }
         echo "available database versions\n";
         $versions = listVersionsFromDB($db, 'db_backend');
     if (empty($versions))
         {
        echo "database no successful versions found\n";
        }
     else
         {
     foreach ($versions as $i => $v)
        {
        echo "[$i] version $v\n";
        }
        echo "enter the number of the database version to roll back to ";
        $choice = trim(fgets(STDIN));
        if (ctype_digit($choice) && isset($versions[$choice]))
        {
        $version = $versions[$choice];
        deployDbmqVersion($dbmqQA, $version);
        deployDbmqVersion($dbmqProd, $version);
        }
        else
        {
        echo "database invalid selection\n";
        }
            }
        echo "rollback complete\n";
     }

    $callback = function ($msg) use ($db, $host)
        { 
    echo "prod message {$msg->body}\n";
    $data = json_decode($msg->body, true);
    if (!$data || !isset($data['qa_status']))
    {
        echo "bad message json\n";
        $msg->ack();
    return;
    }
    if ($data['qa_status'] !== 'pass')
    {
    echo "qa failed initiating rollback\n";
     performRollback($host);
    $msg->ack();
    return;
    }
    echo "qa passed starting production deployment\n";
    $status = "success";

    if ($status === "success")
    {
        $dmzVer = lastVersion($db, 'dmz_bundle');
        if ($dmzVer <= 0)
    {
     $status = "error";
     }
    else
    {
    $dmzProd = $host['prod dmz'];
    $status = deployDmzVersion($dmzProd, $dmzVer);
     }
        }
    if ($status === "success")
    {
     $frontVer = lastVersion($db, 'frontend_webapp');
    if ($frontVer <= 0)
    {
        $status = "error";
        }
     else
     {
     $frontProd = $host['prod frontend'];
     $status = deployFrontendVersion($frontProd, $frontVer);
     }
     }
     if ($status === "success")
     {
    $dbVer = lastVersion($db, 'db_backend');
    if ($dbVer <= 0)
     {
         $status = "error";
    }
    else
     {
     $dbmqProd = $host['prod database'];
     $status = deployDbmqVersion($dbmqProd, $dbVer);
     }
     }
     if ($status === "success")
     {
        echo "all deployments completed\n";
     }
     else
     {
       echo "deployments failed\n";
     }
    $msg->ack();
    };
    $channel->basic_consume('prod_request', '', false, false, false, false, $callback);
    while ($channel->is_consuming())
    {
     $channel->wait();
    }

