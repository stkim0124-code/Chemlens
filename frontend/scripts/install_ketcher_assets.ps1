$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$dest = Join-Path $root 'public\\ketcher'
$tmp  = Join-Path $env:TEMP ('ketcher-standalone-' + [Guid]::NewGuid().ToString())
$zip  = Join-Path $tmp 'ketcher-standalone.zip'

New-Item -ItemType Directory -Force -Path $tmp | Out-Null
New-Item -ItemType Directory -Force -Path $dest | Out-Null

$uri = 'https://github.com/epam/ketcher/releases/latest/download/ketcher-standalone.zip'
Write-Host "Downloading: $uri"

if (Get-Command curl.exe -ErrorAction SilentlyContinue) {
  & curl.exe -L -o $zip $uri
} else {
  Invoke-WebRequest -Uri $uri -OutFile $zip
}

Write-Host "Extracting to temp..."
Expand-Archive -Path $zip -DestinationPath $tmp -Force

$src = Get-ChildItem -Path $tmp -Recurse -File | Where-Object { $_.Name -ieq 'standalone.js' } | Select-Object -First 1
if (-not $src) {
  throw "standalone.js not found inside downloaded archive. Archive layout may have changed."
}
$srcRoot = Split-Path -Parent $src.FullName

Write-Host "Copying assets into: $dest"
Get-ChildItem -Path $srcRoot -Force | ForEach-Object {
  if ($_.Name -ieq 'index.html') { return }
  Copy-Item -Path $_.FullName -Destination (Join-Path $dest $_.Name) -Recurse -Force
}

Write-Host "Done. Restart dev server if running."
