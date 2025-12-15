<?php
$ch = curl_init();
$APIKeyFile = fopen("../doNotPushToGit/mailgunAPIKey", "r") or die("Unable to open file!");
$mailgunAPIKey = fread($APIKeyFile,filesize("../doNotPushToGit/mailgunAPIKey"));
fclose($APIKeyFile);
curl_setopt($ch, CURLOPT_URL, "https://api.mailgun.net/v3/YOUR_SANDBOX_DOMAIN/messages");
curl_setopt($ch, CURLOPT_POST, true);
curl_setopt($ch, CURLOPT_USERPWD, "api:" . $APIKeyFile);
curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query([
    'from' => 'Test <benjamin@pivotpoint.com>',
    'to' => 'bp455@njit.edu',
    'subject' => 'Hello!',
    'text' => 'Test message'
]));
curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);

$response = curl_exec($ch);
$status = curl_getinfo($ch, CURLINFO_HTTP_CODE);
echo "Status: $status\n";
curl_close($ch);
?>