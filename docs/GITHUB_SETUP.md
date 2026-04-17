# GitHub Setup

Este projeto ja esta preparado para ser publicado primeiro no GitHub pessoal e depois espelhado ou migrado para o GitHub da empresa.

## Estrategia recomendada

- Use `origin` para o repositorio pessoal.
- Use `company` para o repositorio da empresa.
- Mantenha a branch principal como `main`.
- Evite salvar credenciais no codigo; continue usando `.env.docker` local e variaveis de ambiente nos ambientes corporativos.

## O que ja ficou pronto

- Repositorio Git inicializado localmente.
- `.gitignore` protegendo `.env.docker`, artefatos locais, logs, `target/` e `__pycache__/`.
- `.env.docker` mantido fora do versionamento para nao expor segredos.
- Script `scripts/setup_git_remotes.ps1` para configurar remotos de forma padronizada.

## Fluxo para o GitHub pessoal

1. Crie um repositorio vazio no GitHub pessoal.
2. Rode o script:

```powershell
.\scripts\setup_git_remotes.ps1 -PersonalRepoUrl "https://github.com/SEU_USUARIO/etl_bi.git"
```

3. Faça o primeiro commit e push:

```powershell
git add .
git commit -m "chore: estrutura inicial do projeto etl_bi"
git push -u origin main
```

## Fluxo para adicionar o GitHub da empresa depois

Quando o repositorio corporativo existir, adicione tambem o remoto `company`:

```powershell
.\scripts\setup_git_remotes.ps1 `
  -PersonalRepoUrl "https://github.com/SEU_USUARIO/etl_bi.git" `
  -CompanyRepoUrl "https://github.com/EMPRESA/etl_bi.git"
```

Depois, publique a mesma branch no remoto corporativo:

```powershell
git push -u company main
```

## Estrategias de uso futuro

- Se o pessoal continuar como espelho: faca push para `origin` e `company`.
- Se a empresa virar o remoto principal: mantenha `company` como destino oficial e deixe `origin` apenas como backup.
- Se quiser trocar o principal mais tarde, basta atualizar a documentacao interna e o fluxo operacional; o repositorio local pode manter ambos os remotos.

## Checklist antes de publicar

- Confirmar que `.env.docker` nao esta stageado.
- Confirmar que `dbt/**/target/` e `dbt/**/logs/` nao entraram no commit.
- Revisar `README.md` e nomes de projeto/empresa antes do remoto corporativo.
- Revisar se `profiles.yml` continua lendo credenciais apenas de variaveis de ambiente.
