'use strict'


const notebookHeader = 
`<!DOCTYPE html>
<html>
    <head>
        <title>Blank Kajero notebook</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <meta http-equiv="content-type" content="text/html; charset=UTF8">
        <link rel="stylesheet" href="dist/main.css">
        <meta charset="utf-8" />
    </head>
    <body>
        <script type="text/markdown" id="kajero-md">
`,
notebookFooter = `
</script>
<div id="kajero"></div>
<script type="text/javascript" src="dist/bundle.js"></script>
</body>
</html>
`


module.exports = function notebookWriter(lines) {
    if (Array.isArray(lines))
    lines = lines.join('\n')
    return notebookHeader + lines + notebookFooter
} 