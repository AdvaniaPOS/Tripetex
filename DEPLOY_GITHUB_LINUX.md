# Deploy via GitHub to Linux (Recommended)

This setup gives you:
- Backup in GitHub
- Safe rollback through git history
- Repeatable deploys on your Linux server

## 1) One-time on local machine

Initialize git and push to your repo:

```powershell
cd "c:\Users\JonSigurdarson\OneDrive - Advania\Desktop\Tripletex - Susoft"
git init
git branch -M main
git remote add origin https://github.com/AdvaniaPOS/Tripetex.git
git add .
git commit -m "Initial TT-Susoft sync app"
git push -u origin main
```

## 2) One-time on Linux server

Clone repository:

```bash
cd ~
git clone https://github.com/AdvaniaPOS/Tripetex.git Tripletex
cd Tripletex
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Create production environment file (do not commit this):

```bash
cp .env.example .env
nano .env
```

Set at least:
- `TRIPLETEX_BASE_URL=https://tripletex.no/v2`
- `TRIPLETEX_CONSUMER_TOKEN`
- `TRIPLETEX_EMPLOYEE_TOKEN`
- `SUSOFT_SHOP_URL_KEY`
- `SUSOFT_USERNAME`
- `SUSOFT_PASSWORD`
- `WEBHOOK_SHARED_SECRET`

## 3) systemd service (example)

Create service file:

```bash
sudo nano /etc/systemd/system/tt-susoft.service
```

Paste:

```ini
[Unit]
Description=TT-Susoft FastAPI service
After=network.target

[Service]
Type=simple
User=poshubadmin
WorkingDirectory=/home/poshubadmin/Tripletex
EnvironmentFile=/home/poshubadmin/Tripletex/.env
ExecStart=/home/poshubadmin/Tripletex/.venv/bin/python -m uvicorn src.main:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable tt-susoft.service
sudo systemctl start tt-susoft.service
sudo systemctl status tt-susoft.service --no-pager -l
```

## 4) Deploy updates (daily flow)

After you push new code to GitHub:

```bash
cd ~/Tripletex
chmod +x scripts/deploy_pull.sh
BRANCH=main SERVICE_NAME=tt-susoft.service ./scripts/deploy_pull.sh
```

## 5) Rollback fast

If something breaks after deploy:

```bash
cd ~/Tripletex
git log --oneline -n 10
git checkout <last-good-commit>
sudo systemctl restart tt-susoft.service
```

Then make a proper fix and push forward.

## 6) Endringslogg (siste leveranser)

### 2026-06-24

1. Fullfort direkte-salg oppgjor med bedre transparens i dashboard:
	- settlement runs + detaljvisning
	- payload med payment breakdown, ordredetaljer, posting-resultat
	- valgfri auto-bokforing i Tripletex (inbox eller auto)
2. Innfort salgsdag-cutoff (default 05:00) for oppgjor.
3. Lagt til betalingstype-regler per tenant (JSON), for eksempel kontant, kort, Vipps, gavekort og kortsubtyper.
4. Lagt til redigering av eksisterende tenants i UI, inkludert robust DB-backfill av nye kolonner for eldre installasjoner.
5. Implementert TT som master for produktsynk mot Susoft:
	- ny synk: TT produkt -> Susoft produkt (opprett/oppdater)
	- felter: varenavn, mva, pris, aktiv/inaktiv
	- kobling via alternativeId: TT:{tripletex_product_id}
6. Implementert kategori-matching fra TT inntektskonto til Susoft kategori med overstyring:
	- matcher mot Susoft kategori via id eller navn
	- kan opprette ny kategori ved execute hvis ingen match finnes
	- regler lagres per tenant i product_sync_category_rules_json
7. Nytt API-endepunkt for produktsynk:
	- POST /api/tenants/{tenant_key}/products/sync-from-tripletex?execute=false|true&limit=...
8. Oppdatert dashboard med:
	- Sync Products (Preview/Execute)-knapper
	- egen resultattabell for produktsynk per produkt (status, kategori, konto, match)
9. Ny CLI-kommando for produktsynk:
	- python manage.py sync-products --tenant-key <key> [--execute] [--limit 200]