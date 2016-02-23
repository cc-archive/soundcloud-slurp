<?php

/*

Make sure you:

   CREATE TABLE download_ids_offsets (
       id             INT        UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
       ids_offset     BIGINT     UNSIGNED NOT NULL
   );
   INSERT INTO download_ids_offsets (ids_offset) VALUES (1);

We need that initial (non-)offset.

*/

require(__DIR__ . '/config.php');

$requestor = $_SERVER['REMOTE_ADDR'];

// Fetch and update the urls range

global $dbh;

$dbh = new PDO($DBDSN, $DBUSER, $DBPASSWORD);

// Where did the last downloader finish?

$offset_statement = $dbh->prepare("SELECT ids_offset FROM download_ids_offsets
                                   ORDER BY id DESC
                                   LIMIT 1");
$ok = $offset_statement->execute();
if ($ok === false) {
    error_log("Couldn't execute select on count to continue for " . $requestor);
    http_response_code(500);
    exit;
}
$start_offset = $offset_statement->fetchColumn();
if ($start_offset === false) {
    error_log("Couldn't get count to continue column for " . $requestor);
    http_response_code(500);
    exit;
}

// exclusive range

$end_offset = $start_offset + $IDS_QUANTITY;

// Get the ids and urls from the database

$tracks_info = false;
$select_urls_statement = $dbh->prepare("SELECT id, download_url
                                       FROM soundcloud_tracks_by_date
                                       WHERE id >= " . $start_offset
                                    . " AND id  < " . $end_offset);
$ok = $select_urls_statement->execute();
if ($ok === false) {
    error_log("Couldn't select tacks for " . $requestor);
    http_response_code(500);
    exit;
}

// Fetch key/value rows to encode directly as json

$tracks = $select_urls_statement->fetchAll(PDO::FETCH_ASSOC);
if ($tracks === false) {
    error_log("Couldn't fetch all tracks for " . $requestor);
    http_response_code(500);
    exit;
}

// Format as json, adding useful info

if (count($tracks) > 0) {
    echo json_encode([
        'urls' => $tracks,
        'config' => [
            'client_id' => $CONFIG_CLIENT_ID
        ]
    ]);
} else {
    error_log("Couldn't fetch all tracks for " . $requestor);
    http_response_code(404);
    echo json_encode([]);
}

// Store the offset that the next task should start from

$store_offset_statement = $dbh->prepare("INSERT INTO download_ids_offsets
                                             (ids_offset)
                                             VALUES (:offset)");
$ok = $store_offset_statement->execute([':offset' => $end_offset]);
if ($ok === false) {
    error_log("Couldn't save next offset (" . $end_offset . ") for "
            . $requestor);
    http_response_code(500);
    exit;
}
