<!doctype html>
<html lang="en">
  <head>
    <meta name="viewport" content="width=device-width, initial-scale=1.0, minimum-scale=1.0, maximum-scale=1.0"/>
    <link href='https://fonts.googleapis.com/css?family=Gochi+Hand' rel='stylesheet' type='text/css'>    
    <meta charset="utf-8">
    <title>Count</title>
    <meta http-equiv="refresh" content="600; URL=http://107.170.230.223/count.php">
    <body>
      <h1 style="margin-left: 243px"><span style="font-family: 'Gochi Hand', 'Comic Sans', 'Chalkboard', 'Patrick Hand', 'Marker Felt', cursive; letter-spacing: 0.1em; padding: 10px; border-radius: 1em;border: 2px solid black;"><?= number_format(file_get_contents('trackdatacount')); ?></span></h1>
      <h1 style="margin-left: 243px">/</h1>
      <img src="Count_von_Count_kneeling.png" style="margin-top: -30px;">
    </body>
</html>
