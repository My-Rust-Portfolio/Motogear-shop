$ErrorActionPreference = 'Stop'

$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$imageDirectory = Join-Path $scriptDirectory 'images'
New-Item -ItemType Directory -Force -Path $imageDirectory | Out-Null

$npxCommand = if ($env:OS -eq 'Windows_NT') { 'npx.cmd' } else { 'npx' }
$mermaidCli = '@mermaid-js/mermaid-cli@11.17.0'

Get-ChildItem -LiteralPath $scriptDirectory -Filter '*.mmd' -File | Sort-Object Name | ForEach-Object {
    $source = $_.FullName
    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($_.Name)
    foreach ($extension in @('svg', 'png')) {
        $output = Join-Path $imageDirectory "$baseName.$extension"
        & $npxCommand --yes --package $mermaidCli -- mmdc --input $source --output $output --theme neutral --backgroundColor white
        if ($LASTEXITCODE -ne 0) {
            throw "Mermaid rendering failed for $($baseName).$extension (exit $LASTEXITCODE)."
        }
    }
    Write-Host "Rendered $baseName.svg and $baseName.png"
}
