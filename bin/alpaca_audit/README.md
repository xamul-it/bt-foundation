# alpaca_audit — Audit portafoglio Alpaca

Strumenti per scaricare e analizzare lo stato del portafoglio Alpaca (paper e live):
equity curve, ordini, posizioni aperte, leva, costi di margine.

---

## Utilizzo rapido

```bash
cd /home/htpc/backtrader
source bt-core/.venv/bin/activate

# --- PA1 (paper account 1) ---
set -a; source env/pa1; set +a
python bin/alpaca_audit/fetch_data.py       --account pa1
python bin/alpaca_audit/analyze.py          --account pa1
python bin/alpaca_audit/leverage_simulation.py --account pa1 [--rate 0.10]

# --- PA2 (paper account 2) ---
set -a; source env/pa2; set +a
python bin/alpaca_audit/fetch_data.py       --account pa2
python bin/alpaca_audit/analyze.py          --account pa2

# --- Account live ---
set -a; source env/live; set +a
python bin/alpaca_audit/fetch_data.py       --account live --live
python bin/alpaca_audit/analyze.py          --account live
```

Output scritto in `bin/alpaca_audit/out/{account}/`:

| File | Contenuto |
|------|-----------|
| `account_snapshot.json` | Snapshot istantaneo (equity, cash, BP, margin) |
| `portfolio_history.csv` | Equity curve giornaliera + P&L |
| `orders.csv` | Tutti gli ordini del periodo |
| `positions.csv` | Posizioni aperte al momento della fetch |

---

## Opzioni script

| Script | Flag | Default | Descrizione |
|--------|------|---------|-------------|
| `fetch_data.py` | `--account NAME` | `default` | Sottodirectory di `out/` |
| | `--days N` | `180` | Giorni di storia |
| | `--live` | assente | Endpoint live invece di paper |
| `analyze.py` | `--account NAME` | `default` | Deve corrispondere a fetch |
| `leverage_simulation.py` | `--account NAME` | `default` | |
| | `--rate FLOAT` | `0.10` | Tasso annuo margin loan (es. 0.12 = 12%) |

---

## Come funziona il margin loan (contabilità Alpaca)

### La formula fondamentale

```
equity = long_market_value + cash
```

Il campo `cash` è determinato **esclusivamente dalle transazioni** (acquisti, vendite,
depositi, prelievi, addebiti interessi). Il prezzo dei titoli **non muove il cash**:
muove solo il `long_market_value`, e quindi l'equity.

### Esempio passo per passo

```
Inizio:         cash = +100k   long_mkt_val =    0k   equity = 100k
Compro 150k:    cash =  -50k   long_mkt_val =  150k   equity = 100k
                margin_loan = 50k  (ho preso 50k a prestito)

Titoli salgono da 150k a 200k:
                cash =  -50k   long_mkt_val =  200k   equity = 150k
                margin_loan = 50k  ← INVARIATO (il prezzo non muove il cash)

Compro altri 30k:
                cash =  -80k   long_mkt_val =  230k   equity = 150k
                margin_loan = 80k  ← sale perché ho comprato, non perché i titoli sono saliti
```

**L'interesse si paga sempre sul cash deficit (loan), mai sul market value.**

### Perché il loan "sembra" crescere con l'apprezzamento

Non cresce da solo. Cresce solo se si fanno nuovi acquisti. Tipicamente:
- Il portafoglio si apprezza → l'investitore si sente più ricco → reinveste
- I nuovi acquisti aumentano il loan
- Il titolo in sé non aumenta il debito: aumenta il collaterale (equity) che lo copre

### Effetto di una discesa dei prezzi

```
Titoli scendono del 20%:   long_mkt_val scende,   cash invariato,   loan invariato
→ equity scende
→ il rischio di margin call aumenta (più vicini al maintenance margin)
→ il loan da pagare non cambia
```

Il pericolo della leva è **asimmetrico sul downside**: il loan resta fisso mentre
il collaterale (titoli) si svaluta. Con leva 2.33x, un calo del ~24% del portafoglio
porta al margin call (equity ≤ 25% del market value, soglia Alpaca standard).

---

## Matrice break-even leva

Return minimo annuo sul portafoglio per coprire il costo degli interessi:
`break_even = (1 − 1/leva) × tasso`

| Leva  |  5%  |  8%  | 10%  | 12%  |
|-------|:----:|:----:|:----:|:----:|
| 1.5x  | 1.7% | 2.7% | 3.3% | 4.0% |
| 2.0x  | 2.5% | 4.0% | 5.0% | 6.0% |
| 2.5x  | 3.0% | 4.8% | 6.0% | 7.2% |
| 3.0x  | 3.3% | 5.3% | 6.7% | 8.0% |

A 2x leva con tasso 10%: bastano +5%/anno sul portafoglio per non distruggere valore.

---

## Strategia di gestione della leva

### Tre zone operative

```
VERDE   leva < 1.5x   — operatività normale, nuove posizioni OK
GIALLA  leva 1.5–2.5x — monitoraggio, nessun nuovo acquisto a debito
ARANCIO leva 2.5–3.0x — riduzione attiva, target 2.0x entro 5 giorni
ROSSO   leva > 3.0x   — chiusura urgente, target < 1.5x
```

### Regole operative

**R1 — Ceiling di leva**
- Leva massima tollerata: **2.5x**
- Se leva > 2.5x dopo la chiusura di mercato:
  vendi la posizione con il peggior rapporto rischio/rendimento
  (non necessariamente la più in perdita) fino a leva ≤ 2.0x.

**R2 — Buffer anti-margin-call**
- Mantieni equity ≥ **37.5% del market value** (1.5× il maintenance margin di Alpaca).
- Corrisponde a un calo massimo sopportabile del portafoglio senza margin call:
  `max_drop = (equity/mkt_val − 0.25) / 0.75`
- Esempio: equity 84k, mkt_val 196k → max_drop = (42.9% − 25%) / 75% ≈ **24%**.
- Se max_drop < 15%: riduzione obbligatoria.

**R3 — Prese di profitto parziali**
- Quando una posizione ha P&L latente > **+50%**: vendi il **30%** della quota.
- Usa i proventi per ridurre il loan, NON per aprire nuove posizioni a debito.
- Obiettivo: trasformare plusvalenza latente in riduzione del costo del debito.

**R4 — Stop-loss per posizione**
- Ogni posizione ha uno stop a **−15% dal prezzo di carico** (non trailing).
- Se il titolo tocca lo stop: chiusura totale della posizione.
- In presenza di leva > 2x, lo stop si stringe a **−10%** (lo stesso drawdown
  sulla posizione vale il doppio sull'equity).

**R5 — Stop complessivo di portafoglio**
- Se l'equity scende del **20% dal massimo** (peak-to-trough): stop a nuovi acquisti.
- Se scende del **30%**: chiudi le posizioni più deboli fino a leva ≤ 1.5x.

**R6 — Monitoraggio costo del debito**
- Ogni mese: verifica che il return del portafoglio (ultimi 30 gg annualizzato)
  superi il break-even di leva.
- Se return mensile annualizzato < costo_interessi per 2 mesi consecutivi:
  riduci il loan del 30% vendendo posizioni.

### Azioni prioritarie su PA1 (situazione attuale)

```
Leva attuale: 2.33x  (zona GIALLA, vicino ad ARANCIO)
AMD:   +96%  → candidata a presa di profitto parziale (30% = ~28 azioni)
              → riduce loan di ~$11k, porta leva a ~2.18x
MU:    +44%  → monitorare, non ancora al 50% trigger
CSCO:  +48%  → monitorare, vicina al trigger
Margine a margin call: −24%  → ok ma non abbondante con titoli tech volatili

Azione immediata se si va live:
  1. Vendi 28 azioni AMD → incassa ~$11.9k → loan da 112k a 100k
  2. Non reinvestire i proventi → leva scende a 2.15x, buffer margin call sale a 26%
```

---

## Struttura file

```
bin/alpaca_audit/
  fetch_data.py          ← scarica dati da API Alpaca
  analyze.py             ← report equity, ordini, posizioni, leva
  leverage_simulation.py ← confronto leva vs capitale proprio, interessi simulati
  README.md              ← questo file
  out/{account}/         ← dati scaricati (git-ignored)
```

## Requisiti

- `alpaca-py >= 0.40` (nel venv bt-core)
- Credenziali in `env/pa1`, `env/pa2`, `env/live`
- Python 3.10+, pandas, numpy
