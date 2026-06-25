#!/usr/bin/env python3
"""
Chiede all'agente pandas (claude --print) se è fattibile portare
after-hours in paper trading per la strategia intraday.HMA.

Il contesto tecnico viene costruito automaticamente leggendo i dati
reali della simulazione overnight già eseguita.

Output: bt-strategy-test/HMA/OVERNIGHT_PAPER_FEASIBILITY.md
"""
import sys
from pathlib import Path

AGENT_ROOT = Path(__file__).resolve().parents[2] / "bt-agent"
HMA_DIR    = Path(__file__).resolve().parent
sys.path.insert(0, str(AGENT_ROOT))
sys.path.insert(0, str(HMA_DIR))

from agent.agent import _claude_ask
from agent.prompts import SYSTEM_PROMPT

# ── Contesto tecnico raccolto dall'analisi overnight ──────────────────────────

CONTEXT = """
## Strategia in esame
- Nome: intraday.HMA (bt-core/strategies/intraday.py)
- Parametri attivi (env/psim): period=16, inverted=True, exitbar=6,
  use_calendar=True, sl_pct=0, tp_pct=0, minutes_before_close=15
- Modalità live: ordini LIMIT, close+revert = 2 op separate, TTL intraday

## Risultati simulazione pandas: intraday vs overnight (3 simboli, 2025-01-02→2025-05-30)

| Simbolo | Modalità   | Trade | Total P&L | Sharpe | SQN  |
|---------|------------|------:|----------:|-------:|-----:|
| NFLX    | intraday   | 2.068 |   +62.97% |  +1.10 | 3.14 |
| NFLX    | overnight  | 6.228 |  +138.62% |  +1.00 | 4.96 |
| CSCO    | intraday   | 2.179 |   +95.37% |  +2.09 | 6.16 |
| CSCO    | overnight  | 6.398 |  +213.01% |  +1.95 | 9.81 |
| KHC     | intraday   | 2.070 |   +83.02% |  +1.88 | 5.39 |
| KHC     | overnight  | 6.192 |  +202.41% |  +2.14 |10.62 |

## Dati tecnici rilevanti

### Flusso dati live (verificato su codice bt-core)
- broker/alpaca_data.py riga 694: StockDataStream(api_key, secret_key, raw_data=False)
  → nessun parametro `feed` esplicito → default SIP
- SIP Alpaca include barre minute AH (16:00–20:00 ET) e pre-market (4:00–9:30 ET)
  MA solo quando c'è volume: barre sparse, molti minuti vuoti
- subscribe_bars() riceve le barre come arrivano → BT le processa normalmente

### Comportamento con use_calendar=False in live/paper
- inValidMarket() non viene chiamato → nessun _closeAll() automatico
- Le barre AH arrivano sparse dal WebSocket SIP
- L'HMA con period=16 su serie sparse (buchi di 30-60 minuti) produce
  transizioni artificiali: ogni gap è una "inversione di trend" per il calcolo
- Gli ordini LIMIT vengono emessi al close della barra AH sparse
  → prezzi con spread bid/ask molto ampio in AH
  → fill incerti: possono non essere eseguiti o eseguiti fuori dal prezzo

### Dati storici disponibili per la simulazione
- CSV in config-common/data/minutes/alpaca/ → solo barre RTH (9:30–16:00)
- Nessun dato AH/PM storico disponibile → simulazione overnight incompleta
- Il "gap" nella simulazione è close[15:59] → open[9:30 next day]: un salto netto
  senza vedere le barre AH intermediate

### Anatomia P&L overnight (dati reali)
- 1.7% dei trade attraversa il confine di giornata (312 su 18.818)
- Contributo overnight: +33.5% di P&L totale con WR 64.4%
- Il gap RTH close→next open vale in mediana il 37.6% del P&L del singolo trade overnight
- Esempi estremi: KHC 22-apr, il gap vale il 363% del trade (posizione intraday
  sarebbe in perdita, overnight diventa profittevole solo grazie al gap)

### Bias dataset 2025
- Periodo: gen–mag 2025, forte trend rialzista NASDAQ
- Long overnight hanno catturato sistematicamente gap-up mattutini
- Non testato su periodi ribassisti

### Vincoli Alpaca paper trading
- Ordini limit eseguiti su prezzi bid/ask simulati
- In AH il broker simulation usa prezzi last trade, non mid-market
- Nessun meccanismo di margin call su paper, ma in live Reg-T overnight = 50%
"""

QUESTION = """
Sei l'agente specializzato nella simulazione pandas della strategia intraday.HMA
e nella sua portabilità al paper/live trading su Alpaca.

Sulla base del contesto tecnico fornito, rispondi a questa domanda:

**È fattibile portare after-hours in paper trading per questa strategia?**

Struttura la risposta così:
1. Verdetto immediato (sì/no/condizionato) con motivazione in 2 righe
2. Problemi tecnici concreti da risolvere (numerati, con impatto stimato)
3. Cosa deve cambiare nella strategia BT per funzionare in AH
4. Cosa deve cambiare nella simulazione pandas per validare il comportamento AH
5. Stima dello sforzo implementativo (giorni/settimane)
6. Raccomandazione finale: conviene o è meglio ottimizzare intraday?

Sii diretto e quantitativo dove possibile. Non ripetere il contesto.
"""

OUT_FILE = HMA_DIR / "OVERNIGHT_PAPER_FEASIBILITY.md"


def main():
    print("Invio richiesta all'agente (claude --print)...")
    print("Questo richiede 30-60 secondi.\n")

    response = _claude_ask(QUESTION, context=CONTEXT)

    md = f"""# HMA After-Hours in Paper Trading — Parere dell'Agente

*Risposta generata da `claude --print` (BTPandasAgent) su richiesta di analisi di fattibilità*

---

{response}

---

*Contesto tecnico usato: dati simulazione overnight NFLX/CSCO/KHC 2025-01-02→2025-05-30,*
*verifica codice broker/alpaca_data.py (StockDataStream SIP default)*
"""

    OUT_FILE.write_text(md, encoding="utf-8")
    print(f"Parere salvato in: {OUT_FILE}")
    print("\n" + "─" * 72)
    print(response)
    print("─" * 72)


if __name__ == "__main__":
    main()
