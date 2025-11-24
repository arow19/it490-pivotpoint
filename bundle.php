<?php
require __DIR__ . '/vendor/autoload.php';
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;


$config = parse_ini_file(__DIR__ . '/deploy_config.ini');
if ($config === false) {
    fwrite(STDERR, "[ERROR] Could not read " . __DIR__ . "/deploy_config.ini\n");
    exit(1);
}


$bundle    = 'frontend_webapp';
$version   = 'testing';
$target    = 'qa';
$component = 'frontend/php-apache';


$srcDir       = '/var/www/sample';
$localArchive = "/tmp/{$bundle}_v{$version}.zip";


$remoteHost = '10.227.85.197';
$remoteUser = 'deploy';
$remoteDir  = '/home/deploy/archive';
$remotePath = "{$remoteDir}/{$bundle}_v{$version}.zip";


function run_or_fail(string $cmd): void {
    echo "[CMD] $cmd\n";
    exec($cmd . ' 2>&1', $out, $rc);
    if (!empty($out)) echo implode("\n", $out) . "\n";
    if ($rc !== 0) {
        fwrite(STDERR, "[ERROR] Command failed: $cmd\n");
        exit($rc ?: 1);
    }
}


function ensure_dir(string $path): void {
    if (!is_dir($path)) {
        fwrite(STDERR, "[ERROR] Directory not found: $path\n");
        exit(1);
    }
}


ensure_dir($srcDir);
@unlink($localArchive);


run_or_fail(sprintf(
    "cd %s && zip -qr %s . -i '*.php' '*.html'",
    escapeshellarg($srcDir),
    escapeshellarg($localArchive)
));


if (!file_exists($localArchive) || filesize($localArchive) === 0) {
    fwrite(STDERR, "[ERROR] Failed to create archive: $localArchive\n");
    exit(1);
}


run_or_fail(sprintf(
    "ssh -o StrictHostKeyChecking=no %s@%s %s",
    escapeshellarg($remoteUser),
    escapeshellarg($remoteHost),
    escapeshellarg('mkdir -p ' . $remoteDir)
));


run_or_fail(sprintf(
    "scp -o StrictHostKeyChecking=no %s %s@%s:%s",
    escapeshellarg($localArchive),
    escapeshellarg($remoteUser),
    escapeshellarg($remoteHost),
    escapeshellarg($remoteDir . '/')
));


$data = [
    'bundle_name'  => $bundle,
    'version'      => $version,
    'target_env'   => $target,
    'component'    => $component,
    'archive_path' => $remotePath,
    'source_dir'   => $srcDir,
];


try {
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
        120.0,
        600.0,
        null,
        true,
        240

    );


    $ch = $conn->channel();
    $ch->queue_declare('deploy_request', false, true, false, false);


    $msg = new AMQPMessage(json_encode($data, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE));
    $ch->basic_publish($msg, '', 'deploy_request');


    echo "Sent package {$bundle} v{$version} to deployment queue.\n";
    echo "Uploaded archive: {$remotePath}\n";


    $ch->close();
    $conn->close();
} catch (Throwable $e) {
    fwrite(STDERR, "[ERROR] RabbitMQ publish failed: " . $e->getMessage() . "\n");
    exit(1);
}

