[CmdletBinding(SupportsShouldProcess=$true)]
param(
  [ValidateSet("preview","apply")] [string]$Mode = "preview",
  [switch]$MoveVenv,          # 可选：把 .venv 挪到 _trash
  [switch]$MovePatchArtifacts,# 可选：把 patch*.py / patch*_files / README_patch*.txt 挪到 _trash
  [int]$KeepTrashDays = 14    # 清理多少天前的 _trash_*
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# 让 -WhatIf 生效：preview=只预演不动文件；apply=真执行
if ($Mode -eq "preview") { $WhatIfPreference = $true } else { $WhatIfPreference = $false }

# 定位到项目根目录（脚本在 scripts 下）
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $Root

$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$trash = Join-Path $Root ("_trash_" + $ts)
New-Item -ItemType Directory -Force -Path $trash | Out-Null

Write-Host "Root  : $Root"
Write-Host "Trash : $trash"
Write-Host "Mode  : $Mode"

function Ensure-Dir([string]$p) {
  if (!(Test-Path $p)) { New-Item -ItemType Directory -Force -Path $p | Out-Null }
}

# 1) 删除 Python 缓存
Write-Host "`n[1] Remove python caches..."
Get-ChildItem -Recurse -Force -Directory -Filter "__pycache__" -ErrorAction SilentlyContinue |
  Remove-Item -Recurse -Force

Get-ChildItem -Recurse -Force -File -Include *.pyc,*.pyo -ErrorAction SilentlyContinue |
  Remove-Item -Force

# 2) 清空 logs / reports（保留目录）
Write-Host "`n[2] Clean logs/ reports contents (keep dirs)..."
foreach ($d in @("logs","reports")) {
  Ensure-Dir $d
  Get-ChildItem $d -Force -ErrorAction SilentlyContinue |
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
}

# 3) 可选：把 patch 产物挪到 trash（通常已经应用过就不用留在根目录）
if ($MovePatchArtifacts) {
  Write-Host "`n[3] Move patch artifacts to trash..."
  $moveItems = @(
    "patch13_apply.py","patch14_apply.py","patch15_apply.py",
    "patch13_files","patch14_files","patch15_files",
    "README_patch13.txt","README_patch14.txt","README_patch15.txt"
  )
  foreach ($it in $moveItems) {
    if (Test-Path $it) {
      Move-Item -Force $it $trash
    }
  }
}

# 4) 可选：把 .venv 挪到 trash（仓库建议永远不提交 venv）
if ($MoveVenv) {
  Write-Host "`n[4] Move .venv to trash..."
  if (Test-Path ".venv") { Move-Item -Force ".venv" $trash }
}

# 5) 清理过老的 _trash_*
Write-Host "`n[5] Prune old _trash_* (>$KeepTrashDays days)..."
Get-ChildItem -Directory -Force -Filter "_trash_*" -ErrorAction SilentlyContinue |
  Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-$KeepTrashDays) } |
  Remove-Item -Recurse -Force

Write-Host "`nDONE. If preview looked good, rerun with -Mode apply."
