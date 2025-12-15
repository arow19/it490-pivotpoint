<?php
require __DIR__ . '/vendor/autoload.php';

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

$conf = parse_ini_file(__DIR__ . '/deploy_config.ini');

if ($conf === false) 
    {
    echo "no config\n";
    exit(1);
}

$bundle = 'dmz_bundle';
$version = 'testing';
$target = 'qa';
$vm = 'dmz';
$stockInfoDir = '/home/andrew/it490-pivotpoint/StockInfo';
$emailsDir = '/home/andrew/it490-pivotpoint/emails';
$tmpDir = '/tmp/bundle_dmz_tmp';
$localArchive = "/tmp/{$bundle}_v{$version}.zip";
$remoteHost = $conf['rabbitHost'];
$remoteUser = 'deploy';
$remoteDir = '/home/deploy/archive';
$remotePath = "{$remoteDir}/{$bundle}_v{$version}.zip";

$deployPass   = $conf['deployPass'] ?? '';
if ($deployPass === '') 
    {
    echo "no deployPass\n";
    exit(1);
}

function run(string $cmd)
{
    exec($cmd, $output, $resultCode);
    if ($resultCode !== 0)
         {
        exit($resultCode);
    }
}

function checkDir(string $path)
{
    if (!is_dir($path)) 
        {
        echo "directory does not exist";
        exit(1);
    }
}


echo "packaging\n";

checkDir($stockInfoDir);
checkDir($emailsDir);

run("rm -rf $tmpDir");
run("mkdir -p $tmpDir/StockInfo $tmpDir/emails");

echo "copying\n";

run("cp -r $stockInfoDir/. $tmpDir/StockInfo/");
run("cp $emailsDir/*.php $tmpDir/emails/ || true");

if (file_exists($localArchive)) 
    {
    unlink($localArchive);
}

echo "zipping\n";

$cmdZip = "cd $tmpDir && zip -qry $localArchive .";
run($cmdZip);

if (!file_exists($localArchive) || filesize($localArchive) === 0) 
    {
    echo "bad zip\n";
    exit(1);
}

echo "uploading\n";

$pass = escapeshellarg($deployPass);
$user = escapeshellarg($remoteUser);
$host = escapeshellarg($remoteHost);
$dir = escapeshellarg($remoteDir);
$file = escapeshellarg($localArchive);

$cmdMkdir = "sshpass -p $pass ssh -o StrictHostKeyChecking=no $user@$host mkdir -p $dir";
run($cmdMkdir);

$cmdUpload = "sshpass -p $pass scp -o StrictHostKeyChecking=no $file $user@$host:$dir/";
run($cmdUpload);

$data = [
    'bundle_name' => $bundle,
    'version' => $version,
    'target_env' => $target,
    'vm' => $vm,
    'archive_path' => $remotePath,
    'source_dirs' => [$stockInfoDir, $emailsDir],
];

echo "sending\n";

try {
    $connection = new AMQPStreamConnection($conf['rabbitmq']['rabbitHost'], 5672, $conf['rabbitmq']['rabbitUser'], $conf['rabbitmq']['rabbitPass'], $conf['rabbitmq']['vhost'], false, 'AMQPLAIN', null, 'en_US', 60.0, 180.0, null, false, 0);
    $channel = $connection->channel();
    $channel->queue_declare('deploy_request', false, true, false, false);
    $json = json_encode($data, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
    $msg  = new AMQPMessage($json);
    $channel->basic_publish($msg, '', 'deploy_request');
    $channel->close();
    $connection->close();

} catch (Throwable $exception) {
    echo "rabbitmq publish fail\n";
    exit(1);
}

echo "done\n";

