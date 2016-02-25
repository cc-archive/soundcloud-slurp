<?php

/*

   CREATE TABLE track_original_filenames (
       id        INT              UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
       track_id  INT              UNSIGNED UNIQUE NOT NULL,
       filename  varchar(255)     NOT NULL
   );
   CREATE INDEX track_id_index on track_original_filenames (track_id);

*/

require(__DIR__ . '/config.php');

$requestor = $_SERVER['REMOTE_ADDR'];

// Security figleaf.

if (! isset($_REQUEST['pass'])) {
    http_response_code(403);
    exit('Bad pass.');
}
$passkey = $_REQUEST['pass'];
if ($passkey != $FILENAME_INSERT_PASSKEY) {
    http_response_code(400);
    exit('Bad pass.');
}

// Get the params.

if (! isset($_REQUEST['track_id'])) {
    http_response_code(403);
    exit('No track_id.');
}
$track_id = intval($_REQUEST['track_id']);

if (! isset($_REQUEST['filename'])) {
    http_response_code(403);
    exit('No filename.');
}
$filename = $_REQUEST['filename'];


// Insert

global $dbh;

$dbh = new PDO($DBDSN, $DBUSER, $DBPASSWORD);

$insert_statement = $dbh->prepare("INSERT IGNORE INTO track_original_filenames
                                       (track_id, filename)
                                       VALUES (:track_id, :filename)");
$ok = $insert_statement->execute([':track_id' => $track_id,
                                  ':filename' => $filename]);
if ($ok === false) {
    error_log('Couldn\'t save filename "' . $filename . '" for ' . $track_id);
    http_response_code(500);
    exit;
}
