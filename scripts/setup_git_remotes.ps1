param(
    [Parameter(Mandatory = $false)]
    [string]$PersonalRepoUrl,

    [Parameter(Mandatory = $false)]
    [string]$CompanyRepoUrl
)

$projectRoot = Join-Path $PSScriptRoot ".."
$projectRoot = [System.IO.Path]::GetFullPath($projectRoot)

function Set-RemoteUrl {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RemoteName,

        [Parameter(Mandatory = $true)]
        [string]$RemoteUrl
    )

    $remoteExists = git -C $projectRoot remote get-url $RemoteName 2>$null

    if ($LASTEXITCODE -eq 0) {
        git -C $projectRoot remote set-url $RemoteName $RemoteUrl
        Write-Host "Remote '$RemoteName' atualizado para $RemoteUrl"
    }
    else {
        git -C $projectRoot remote add $RemoteName $RemoteUrl
        Write-Host "Remote '$RemoteName' criado com $RemoteUrl"
    }
}

if (-not $PersonalRepoUrl -and -not $CompanyRepoUrl) {
    Write-Host "Informe ao menos um remoto."
    Write-Host ""
    Write-Host "Exemplos:"
    Write-Host '.\scripts\setup_git_remotes.ps1 -PersonalRepoUrl "https://github.com/SEU_USUARIO/etl_bi.git"'
    Write-Host '.\scripts\setup_git_remotes.ps1 -PersonalRepoUrl "https://github.com/SEU_USUARIO/etl_bi.git" -CompanyRepoUrl "https://github.com/EMPRESA/etl_bi.git"'
    exit 1
}

if ($PersonalRepoUrl) {
    Set-RemoteUrl -RemoteName "origin" -RemoteUrl $PersonalRepoUrl
}

if ($CompanyRepoUrl) {
    Set-RemoteUrl -RemoteName "company" -RemoteUrl $CompanyRepoUrl
}

Write-Host ""
git -C $projectRoot remote -v
