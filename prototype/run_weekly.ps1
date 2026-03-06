$ErrorActionPreference = "Stop"

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $here

if (!(Test-Path ".\\config.json")) {
  Write-Host "config.json not found. Creating from config.example.json"
  Copy-Item ".\\config.example.json" ".\\config.json" -Force
}

python ".\\news_reporter.py" --config ".\\config.json"

