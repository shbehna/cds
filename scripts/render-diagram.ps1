<#
.SYNOPSIS
    Renders the Mermaid diagram of README.md to a PNG, for slides.

.DESCRIPTION
    Uses the Mermaid CLI (downloaded by npx on first use) and writes the image to
    dist/cds-services.png, which git ignores.

.EXAMPLE
    ./scripts/render-diagram.ps1
    ./scripts/render-diagram.ps1 -Background white -Scale 4
#>
param(
    # "transparent", "white" or any CSS color.
    [string]$Background = "transparent",
    # Resolution multiplier: 3 stays sharp when resized in PowerPoint.
    [int]$Scale = 3
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$outDir = Join-Path $root "dist"
New-Item -ItemType Directory -Force $outDir | Out-Null

# From a Markdown input, the CLI renders each mermaid block to <name>-<n>.png.
$output = Join-Path $outDir "cds-services.png"
npx -y @mermaid-js/mermaid-cli -i (Join-Path $root "README.md") -o $output -s $Scale -b $Background
if ($LASTEXITCODE -ne 0) { throw "Mermaid CLI failed with exit code $LASTEXITCODE" }

Move-Item -Force (Join-Path $outDir "cds-services-1.png") $output
Write-Host "Diagram written to $output"
