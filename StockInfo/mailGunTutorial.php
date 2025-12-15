<?php
// Include the Autoloader (see "Libraries" for install instructions)
require '../StockInfo/vendor/autoload.php';

// Use the Mailgun class from mailgun/mailgun-php v4.2
use Mailgun\Mailgun;
$APIKeyFile = fopen("../doNotPushToGit/mailgunAPIKey", "r") or die("Unable to open file!");
$mailgunAPIKey = fread($APIKeyFile,filesize("../doNotPushToGit/mailgunAPIKey"));
fclose($APIKeyFile);

// Instantiate the client.
$mg = Mailgun::create(getenv($mailgunAPIKey) ?: $mailgunAPIKey);
// When you have an EU-domain, you must specify the endpoint:
// $mg = Mailgun::create(getenv('API_KEY') ?: 'API_KEY', 'https://api.eu.mailgun.net');

// Compose and send your message.
$result = $mg->messages()->send(
	'sandboxe29d111e7c1d4e58bb7946a23815eb28.mailgun.org',
	[
		'from' => 'Mailgun Sandbox <postmaster@sandboxe29d111e7c1d4e58bb7946a23815eb28.mailgun.org>',
		'to' => 'Benjamin Pang <bp455@njit.edu>',
		'subject' => 'Hello Benjamin Pang',
		'text' => 'Congratulations Benjamin Pang, you just sent an email with Mailgun! You are truly awesome!'
	]
);

print_r($result->getMessage());

?>