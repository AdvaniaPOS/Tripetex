# Tripletex -> Susoft: Losningsdokumentasjon

## Mål
Hente ordredata fra Tripletex API v2 testmiljo og eksportere JSON klar for videre mapping/import mot Susoft.

## Changelog
### 2026-06-24
1. Ferdigstilte direct-sales oppgjor med egen run-historikk og detaljvisning i dashboard.
2. La inn salgsdag-cutoff (default 05:00) og robust beregning av oppgjor-vindu.
3. La inn bokforingsmodus for oppgjor: inbox (ikke auto-bokfor) eller auto-bokfor i Tripletex.
4. Utvidet oppgjor med detaljer: payment_method_breakdown, direct_sales_orders_detail og posting_result.
5. La inn tenant-redigering i dashboard og sikret startup-backfill av nye tenant-kolonner i eksisterende databaser.
6. Implementerte TT som master for produktsynk til Susoft:
   - henter produkter fra Tripletex
   - finner/oppdaterer/oppretter produkter i Susoft via alternativeId
   - synker navn, mva, pris og aktiv-status
7. Implementerte konto -> kategori-overstyring per tenant via JSON-regler:
   - matcher kategori mot Susoft via kategori-id eller kategorinavn
   - oppretter kategori ved execute hvis ingen match finnes
8. La til API for produktsynk:
   - POST /api/tenants/{tenant_key}/products/sync-from-tripletex
9. La til dashboard-wiring for produktsynk:
   - Sync Products (Preview/Execute)
   - resultat-tabell per produkt med status, kategori, kontokilde og match-metode
10. La til CLI-kommando for produktsynk:
   - manage.py sync-products

### 2026-06-19
1. Etablerte autentisering mot Tripletex testmiljo via session token.
2. Avklarte at ordre hentes med Basic auth-format 0:sessionToken i dette miljoet.
3. Rettet datofiltrering ved a bruke eksklusiv orderDateTo (satt til i morgen for a inkludere dagens ordre).
4. Bekreftet henting av 3 ordrer for kunde 93615342.
5. Utvidet feltutvalg med pris, MVA, rabatt og markup pa ordrelinjer.
6. Genererte eksportfil for videre arbeid mot Susoft.
7. Opprettet automatiseringsskript for fakturering og betalingsregistrering: [tripletex_invoice_payment_flow.py](tripletex_invoice_payment_flow.py).
8. Startet implementasjon av lokal driftstjeneste (uten Docker) med FastAPI dashboard, basic auth og PostgreSQL-tilkobling.
9. Lagt inn første datamodell for multi-tenant runtime (`tenants`, `job_runs`) og admin-CLI for lokal test.
10. Utvidet runtime-modell med `order_sync` og `sync_events` for sporbar sync per tenant.
11. La inn manuell sync-flyt per tenant med Tripletex-henting, order upsert og eventlogging.
12. Eksponerte operativ API for `manual sync`, `order-sync` og `events`.
13. Implementerte faktisk Susoft-push ved execute-modus, med mapping fra Tripletex ordre til Susoft ordre.
14. Implementerte retry-flyt for feilede ordrer med eget API-endepunkt og CLI-kommando.
15. Oppgraderte dashboard med live status, tenant-velger, manuell sync-knapper og tabeller for `order_sync` og `events`.
16. Endret Susoft-opprettelse til POS-flyt (`/order/pos`) slik at ordre behandles i kasseflyten og ikke avsluttes automatisk av integrasjonen.
17. La inn webhook-stotte for Tripletex `order.create` og Susoft betalinger, med delt secret via `X-Webhook-Secret`.
18. Lagt til API og dashboard-stotte for a liste og opprette Tripletex webhook-subscriptions.
19. Avklart at webhook callback ma peke til en offentlig URL, for eksempel via Cloudflare eller en offentlig server.
17. Webhook-stotte er lagt inn for `order.create` og betalingshendelser, men krever offentlig tilgjengelig callback-URL for faktisk drift.

## Hva som er gjort
1. Opprettet testskript for Tripletex i [tripletex_open_orders_test.py](tripletex_open_orders_test.py).
2. Satt opp autentisering via session token fra `/token/session/:create`.
3. Verifisert at kall mot `/order` fungerer med Basic auth-format `0:sessionToken` (base64), med fallback fra Bearer.
4. Lagt inn robust feilhåndtering for API-responser.
5. Lagt inn korrekte filtre for ordre:
   - `isSent=true`
   - `isInvoiced=false`
   - `orderDateFrom`
   - `orderDateTo`
6. Avklart at `orderDateTo` er eksklusiv i Tripletex (ma settes til i morgen for a inkludere dagens ordrer).
7. Lagt inn feltutvalg som inkluderer pris, MVA og rabatt pa ordrelinjer.
8. Generert eksportfil [tripletex_orders_for_susoft.json](tripletex_orders_for_susoft.json).

## Viktige tekniske avklaringer
1. Auth:
   - Session token opprettes uten auth-header i `/token/session/:create`.
   - `/order` i dette miljoet godtok Basic med username `0` og password `sessionToken`.
2. Datointervall:
   - `orderDateTo` er "to and excluding" i API-et.
3. Felter:
   - Noen antatte feltnavn (som `unitPrice`) var ugyldige for `OrderLineDTO`.
   - Gyldige felt i bruk na inkluderer:
     - `unitPriceExcludingVatCurrency`
     - `unitPriceIncludingVatCurrency`
     - `amountExcludingVatCurrency`
     - `amountIncludingVatCurrency`
     - `vatType(id,number,name,percentage)`
     - `discount`
     - `markup`

## Resultat
Eksporten inneholder 3 ordrer for kunde `93615342` med ordrelinjer inkludert pris, MVA og rabatt.

## Filer i losningen
1. [tripletex_open_orders_test.py](tripletex_open_orders_test.py)
   - Henter session token.
   - Henter ordrer med riktig auth/filter/felt.
2. [tripletex_orders_for_susoft.json](tripletex_orders_for_susoft.json)
   - Siste eksport fra Tripletex, klar for mapping mot Susoft.
3. [tripletex_invoice_payment_flow.py](tripletex_invoice_payment_flow.py)
   - Fakturerer ordre via `/order/{id}/:invoice`.
   - Registrerer betaling via `/invoice/{id}/:payment`.
   - Stotter `--dry-run` og idempotent gjenbruk av eksisterende invoice ved allerede fakturert ordre.
4. [src/main.py](src/main.py)
   - FastAPI app med:
   - `/health` for service+DB-status.
   - `/api/status`, `/api/tenants`, `/api/order-sync`, `/api/events` for dashboard data.
   - `POST /api/tenants/{tenant_key}/sync/manual` for manuell sync-kjoring.
   - `POST /api/tenants/{tenant_key}/sync/retry-failed` for retry av feilede ordrer.
   - `GET /api/tripletex/webhooks/subscriptions` for a liste Tripletex webhook-subscriptions.
   - `POST /api/tripletex/webhooks/subscriptions/order-create` for a opprette Tripletex `order.create`-subscription.
   - `/webhooks/tripletex/order` som callback for nye Tripletex-ordre.
   - `/webhooks/susoft/payment` som callback for Susoft-betalingshendelser.
   - enkel basic auth for dashboard/API.
5. [manage.py](manage.py)
   - `init-db`, `add-tenant`, `list-tenants`, `seed-job-run`, `manual-sync`, `retry-failed`.
6. [src/sync_service.py](src/sync_service.py)
   - Kjorer manuell tenant-sync, faktisk push mot Susoft og retry av feilede ordrer.
7. [src/susoft_client.py](src/susoft_client.py)
   - Auth mot Susoft (`/user/auth`) og opprettelse av ordre via POS-flyt (`/order/pos`).
8. [src/tripletex_client.py](src/tripletex_client.py)
   - Lokal TT-klient for session token, henting av apne ordrer og administrasjon av webhook-subscriptions.
9. [TripletexApi.txt](TripletexApi.txt)
   - OpenAPI-spesifikasjon brukt for verifisering av felter/endepunkter.
10. [SusoftApi.txt](SusoftApi.txt)
   - Susoft API-referanse for neste mapping-steg.
11. [DEPLOY_GITHUB_LINUX.md](DEPLOY_GITHUB_LINUX.md)
   - Praktisk deploy-oppskrift for GitHub -> Linux med systemd og trygg rollback.

## Kjoring
Kjor skriptet i prosjektmiljo:

```powershell
.venv\Scripts\python.exe tripletex_open_orders_test.py
```

Installer avhengigheter for lokal tjeneste:

```powershell
.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Kopier env-mal og sett egne verdier:

```powershell
Copy-Item .env.example .env
```

Initialiser database:

```powershell
.venv\Scripts\python.exe manage.py init-db
```

Opprett første tenant lokalt:

```powershell
.venv\Scripts\python.exe manage.py add-tenant --tenant-key demo-tenant --name "Demo Tenant"
```

Kjor manuell sync i trygg dry-run:

```powershell
.venv\Scripts\python.exe manage.py manual-sync --tenant-key demo-tenant --dry-run --limit 25
```

Kjor manuell sync med faktisk push til Susoft:

```powershell
.venv\Scripts\python.exe manage.py manual-sync --tenant-key demo-tenant --execute --limit 25
```

Retry feilede ordrer:

```powershell
.venv\Scripts\python.exe manage.py retry-failed --tenant-key demo-tenant --limit 25
```

Start dashboard/service lokalt:

```powershell
.venv\Scripts\python.exe -m uvicorn src.main:app --reload --port 8000
```

Kjor automatiske tester:

```powershell
.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Dashboard og API (basic auth):
- `http://127.0.0.1:8000/`
- `http://127.0.0.1:8000/api/status`
- `http://127.0.0.1:8000/api/tenants`
- `http://127.0.0.1:8000/api/order-sync?tenant_key=demo-tenant`
- `http://127.0.0.1:8000/api/events?tenant_key=demo-tenant`
- `http://127.0.0.1:8000/api/tripletex/webhooks/subscriptions`
- `http://127.0.0.1:8000/api/tripletex/webhooks/subscriptions/order-create?target_url=https://tt.poshub.no/webhooks/tripletex/order`

Nye env-variabler som ma settes for execute mot Susoft:
- `SUSOFT_SHOP_URL_KEY`
- `SUSOFT_USERNAME`
- `SUSOFT_PASSWORD`

For webhook-drift:
- `WEBHOOK_SHARED_SECRET` kan settes for a kreve `X-Webhook-Secret` pa callback.
- Tripletex callback URL ma vaere offentlig tilgjengelig. Lokale `127.0.0.1`-adresser fungerer ikke mot Tripletex.
- Cloudflare Tunnel eller et offentlig domene pa Linux-server er den enkleste veien for produksjon.

Viktig adferd i dagens flyt:
- Integrasjonen sender ikke `payments` i ordrepayload.
- Ordre opprettes via POS-endepunkt i Susoft (`/order/pos`) for videre behandling i POS.
- `TRIPLETEX_CONSUMER_TOKEN`
- `TRIPLETEX_EMPLOYEE_TOKEN`
- Tripletex webhook-subscriptions er tomme helt til de blir opprettet eksplisitt via API eller dashboard.

For a regenerere eksportfilen med samme struktur, kjør skriptet og skriv resultat til JSON (samme oppskrift som brukt i sesjonen).

Eksempel, trygg test uten endringer (dry-run):

```powershell
.venv\Scripts\python.exe tripletex_invoice_payment_flow.py --order-id 210270345 --register-payment --payment-type-id 1 --paid-amount 8737.5 --dry-run
```

## Neste steg mot Susoft
1. Definer endelig felmapping fra Tripletex til Susoft (ordrehode + ordrelinjer).
2. Beregn eksplisitt `vatAmount` per linje ved behov:
   - `vatAmount = amountIncludingVatCurrency - amountExcludingVatCurrency`
3. Bygg eget transform-skript som skriver en ren `susoft_orders_payload.json`.
4. Legg inn idempotensnokkel per ordre (for eksempel Tripletex `order.id`) for trygg synkronisering.

## Notat om sikkerhet
Ikke legg consumer/employee/session tokens i dokumentasjon eller versjonskontroll.
