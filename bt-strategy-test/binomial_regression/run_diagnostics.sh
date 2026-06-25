#!/bin/bash
# Script per eseguire diagnostica completa della strategia regressione

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colori per output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "================================================================================"
echo "DIAGNOSTICA STRATEGIA REGRESSIONE - TEST SUITE"
echo "================================================================================"
echo ""

# Crea cartella logs
mkdir -p logs

# FASE 1: Verifiche critiche
echo "${YELLOW}FASE 1: VERIFICHE CRITICHE${NC}"
echo "--------------------------------------------------------------------------------"

echo ""
echo "[1/2] Verifica calcolo slope..."
python verify_slope_calculation.py | tee logs/verify_slope.log
SLOPE_STATUS=$?

echo ""
echo "[2/2] Verifica forward outcomes..."
python verify_forward_outcomes.py | tee logs/verify_outcomes.log
OUTCOMES_STATUS=$?

echo ""
echo "--------------------------------------------------------------------------------"
echo "RIEPILOGO FASE 1:"
echo "--------------------------------------------------------------------------------"

# Check for errors in logs
SLOPE_ERRORS=$(grep -c "❌ FAIL" logs/verify_slope.log || true)
SLOPE_LOGIC_ERRORS=$(grep -c "LOGIC ERROR" logs/verify_slope.log || true)
OUTCOMES_ERRORS=$(grep -c "❌ FAIL" logs/verify_outcomes.log || true)
LOOKAHEAD_WARNINGS=$(grep -c "LOOK-AHEAD" logs/verify_outcomes.log || true)

if [ "$SLOPE_STATUS" -eq 0 ]; then
    if [ "$SLOPE_ERRORS" -gt 0 ] || [ "$SLOPE_LOGIC_ERRORS" -gt 0 ]; then
        echo -e "${RED}❌ Slope verification: FAIL${NC}"
        echo "   - Numerical errors: $SLOPE_ERRORS"
        echo "   - Logic errors: $SLOPE_LOGIC_ERRORS"
        echo "   AZIONE RICHIESTA: Controlla logs/verify_slope.log e correggi il bug"
        echo "   IMPORTANTE: RICHIEDERÀ RICALCOLO COMPLETO DEI DATI"
    else
        echo -e "${GREEN}✅ Slope verification: PASS${NC}"
    fi
else
    echo -e "${RED}❌ Slope verification: ERROR (exit code $SLOPE_STATUS)${NC}"
fi

if [ "$OUTCOMES_STATUS" -eq 0 ]; then
    if [ "$OUTCOMES_ERRORS" -gt 0 ]; then
        echo -e "${RED}❌ Forward outcomes verification: FAIL${NC}"
        echo "   - Numerical errors: $OUTCOMES_ERRORS"
        echo "   AZIONE RICHIESTA: Controlla logs/verify_outcomes.log e correggi il bug"
        echo "   IMPORTANTE: RICHIEDERÀ RICALCOLO COMPLETO DEI DATI"
    elif [ "$LOOKAHEAD_WARNINGS" -gt 0 ]; then
        echo -e "${YELLOW}⚠️  Forward outcomes verification: WARNING${NC}"
        echo "   - Look-ahead warnings: $LOOKAHEAD_WARNINGS"
        echo "   Controlla logs/verify_outcomes.log per dettagli"
    else
        echo -e "${GREEN}✅ Forward outcomes verification: PASS${NC}"
    fi
else
    echo -e "${RED}❌ Forward outcomes verification: ERROR (exit code $OUTCOMES_STATUS)${NC}"
fi

# Se ci sono errori critici, ferma qui
if [ "$SLOPE_ERRORS" -gt 0 ] || [ "$SLOPE_LOGIC_ERRORS" -gt 0 ] || [ "$OUTCOMES_ERRORS" -gt 0 ]; then
    echo ""
    echo "${RED}================================================================================${NC}"
    echo "${RED}ERRORI CRITICI TROVATI - DIAGNOSTICA INTERROTTA${NC}"
    echo "${RED}================================================================================${NC}"
    echo ""
    echo "Devi correggere i bug prima di procedere con l'analisi timing."
    echo ""
    echo "Passi successivi:"
    echo "1. Leggi i log in logs/verify_slope.log e logs/verify_outcomes.log"
    echo "2. Correggi il codice in 01-fast_backtest.py"
    echo "3. Ricalcola i dati:"
    echo "   python 01-fast_backtest.py"
    echo "   python 02-feature_diagnostics.py"
    echo "   python 03-slice_analisys.py"
    echo "4. Riprova questo script: ./run_diagnostics.sh"
    echo ""
    exit 1
fi

# FASE 2: Analisi timing
echo ""
echo "${YELLOW}FASE 2: ANALISI TIMING HIT TP/SL${NC}"
echo "--------------------------------------------------------------------------------"

echo ""
echo "Esecuzione analyze_hit_timing.py..."
echo "(Questo può richiedere alcuni minuti per processare tutti gli asset)"
echo ""

python analyze_hit_timing.py \
    --tp-pct 0.005 \
    --sl-pct 0.003 \
    --horizons 3,5,6,10,15,30,60 \
    --slope5-min 0.0004 \
    --slope20-min 0.0002 \
    --slope60-min 0.00015 \
    --volz-min 0.85 \
    --quality-window 60 \
    --quality-min 0.5 \
    | tee logs/hit_timing.log

TIMING_STATUS=$?

echo ""
echo "--------------------------------------------------------------------------------"
echo "RIEPILOGO FASE 2:"
echo "--------------------------------------------------------------------------------"

if [ "$TIMING_STATUS" -eq 0 ]; then
    echo -e "${GREEN}✅ Analisi timing completata${NC}"
    echo ""
    echo "Risultati salvati in: results/hit_timing_analysis/"
    echo ""

    # Estrai metriche chiave
    if [ -f "results/hit_timing_analysis/hit_timing_global_stats.csv" ]; then
        echo "METRICHE CHIAVE:"
        echo ""

        # Usa Python per estrarre e formattare i risultati
        python3 << 'PYEOF'
import pandas as pd
import sys

try:
    df = pd.read_csv("results/hit_timing_analysis/hit_timing_global_stats.csv")

    print(f"  Mediana barre a TP:          {df['median_bars_to_tp'].values[0]:.1f}")
    print(f"  Mediana barre a SL:          {df['median_bars_to_sl'].values[0]:.1f}")
    print(f"  Mediana barre a primo hit:   {df['median_bars_to_first_hit'].values[0]:.1f}")
    print(f"  P(hit TP):                   {100 * df['p_hit_tp'].values[0]:.1f}%")
    print(f"  P(hit SL):                   {100 * df['p_hit_sl'].values[0]:.1f}%")
    print(f"  P(no hit):                   {100 * df['p_no_hit'].values[0]:.1f}%")

    # Carica anche summary by horizon
    df_h = pd.read_csv("results/hit_timing_analysis/hit_timing_summary_by_horizon.csv")
    row_6 = df_h[df_h['horizon_bars'] == 6]
    if not row_6.empty:
        print("")
        print(f"  @ 6 MINUTI:")
        print(f"    P(hit TP entro 6m):        {100 * row_6['p_hit_tp_within'].values[0]:.1f}%")
        print(f"    P(hit SL entro 6m):        {100 * row_6['p_hit_sl_within'].values[0]:.1f}%")
        print(f"    P(no close entro 6m):      {100 * row_6['p_no_close_within'].values[0]:.1f}%")

        p_no_close = row_6['p_no_close_within'].values[0]
        if p_no_close > 0.5:
            print("")
            print("  ⚠️  ATTENZIONE: Più del 50% delle posizioni NON si chiude entro 6 minuti!")
            print("      Considera:")
            print("      - Ridurre TP/SL")
            print("      - Aggiungere filtri su volatilità")
            print("      - Restringere a slot orari ad alta volatilità")

except Exception as e:
    print(f"Errore nel leggere i risultati: {e}", file=sys.stderr)
    sys.exit(1)
PYEOF
    fi

else
    echo -e "${RED}❌ Analisi timing fallita (exit code $TIMING_STATUS)${NC}"
    echo "   Controlla logs/hit_timing.log per dettagli"
    exit 1
fi

echo ""
echo "================================================================================"
echo -e "${GREEN}DIAGNOSTICA COMPLETATA${NC}"
echo "================================================================================"
echo ""
echo "Prossimi passi:"
echo ""
echo "1. Rivedi i grafici in results/hit_timing_analysis/"
echo "2. Se troppo poche posizioni si chiudono entro 6 minuti:"
echo "   - Riduci TP/SL"
echo "   - Aggiungi filtri su volatilità realizzata"
echo "   - Restringi a slot orari specifici"
echo "3. Leggi README_DIAGNOSTICS.md per dettagli su fase 2.2 e 2.3"
echo ""
echo "Log salvati in:"
echo "  - logs/verify_slope.log"
echo "  - logs/verify_outcomes.log"
echo "  - logs/hit_timing.log"
echo ""
