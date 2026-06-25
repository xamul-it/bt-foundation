#!/bin/bash
# Monitor live results every 2 minutes

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACK_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$SCRIPT_DIR"
source "$BACK_DIR/bt-core/.venv/bin/activate"

while true; do
    clear
    echo "========================================"
    date
    echo "========================================"

    # Check if process is running
    if ps aux | grep "11-sampled" | grep -v grep > /dev/null; then
        echo "✅ Processo ATTIVO"
    else
        echo "❌ Processo TERMINATO"
        python analyze_partial.py
        break
    fi

    echo ""

    # Show last 3 lines of log
    echo "📝 Ultimi progressi:"
    tail -3 sampled_test_run.log | grep INFO || echo "Caricamento..."

    echo ""

    # Analyze if file exists
    if [ -f sampled_results.csv ]; then
        python analyze_partial.py
    else
        echo "⏳ Nessun risultato salvato ancora..."
    fi

    echo ""
    echo "Prossimo aggiornamento in 120 secondi... (Ctrl+C per interrompere)"
    sleep 120
done
