"""Build a reproducible companion notebook; no simulations run on import."""
from pathlib import Path
import nbformat as nbf

ROOT = Path(__file__).resolve().parent


def main():
    notebook = nbf.v4.new_notebook()
    md, code = nbf.v4.new_markdown_cell, nbf.v4.new_code_cell
    notebook.cells = [
        md('''# Lo studio si adatta ai rendimenti della strategia?

**24 ottobre 2026 — OvernightAH e FlatComposite, dati nativi di backtest.**

La struttura del confronto per sequenze è riutilizzabile. Il candidato a sei
categorie perde la dimensione delle perdite. L'adattamento continuo la conserva,
ma resta insufficiente sullo stress di dispersione: circa **44 gruppi su 100**
segnalati per OvernightAH e **37 su 100** per FlatComposite quando la dispersione
dei blocchi è raddoppiata. Il criterio fissato era almeno 50 su 100.

**Applicazione non pronta.** Questo notebook documenta la verifica, senza modificare
applicazione, profili o strategie. [Conclusioni complete](conclusioni.md) ·
[Protocollo](protocol.md) · [Fonti](sources.md)'''),
        md('''## Contesto e metodo

Si osservano blocchi consecutivi di rendimenti giornalieri del portafoglio,
con finestre 3/5/10/15/20 e confronto congiunto. La UI attuale usa invece il
gain medio per singolo trade: questo studio rende esplicita la diversa unità.

### Ipotesi e limiti

Stime iniziali 2000–2004; normalizzazione 2005–2009; calibrazione 2010–2018;
cronologia valutata 2019–2025. I blocchi storici possono essere dipendenti e
il processo non è assunto invariato per 26 anni. Il ricampionamento controllato
usa interi blocchi di 20 rendimenti del 2010–2018. Il suo caso invariato è una
legge empirica costruita, senza garanzia per il mercato futuro. I periodi reali
non hanno etichette note di cambiamento: i loro segnali non sono falsi allarmi
misurati. Il punteggio è una misura di anomalia, non P(processo invariato | dati).

Il riferimento mobile usa solo i cinque anni precedenti, ricalcolati a inizio
anno. Nessuna componente o soglia è stata ritoccata dopo gli esperimenti.'''),
        code('''import os, sys, json, importlib
from pathlib import Path
os.environ['MPLCONFIGDIR'] = '/tmp/statistical-study-mpl'
ROOT = Path.cwd()
if not (ROOT / 'analyze.py').exists():
    ROOT = Path('/home/htpc/backtrader/research/statistical_process_compatibility_20261024/strategy_adaptation')
sys.path.insert(0, str(ROOT))
import pandas as pd
from IPython.display import display, Image, Markdown
REGENERATE_EXPERIMENT = False
if REGENERATE_EXPERIMENT:
    import analyze
    analyze.main()
import validate, figures, write_notes
validate.main()
figures.main()
write_notes.main()
controlled = pd.read_csv(ROOT/'results/controlled_summary.csv', dtype={'window': str})
historical = pd.read_csv(ROOT/'results/historical_annual.csv', dtype={'window': str})
assessment = json.loads((ROOT/'results/assessment.json').read_text())
assert not assessment['production_application']
print('Criteri diagnostici tutti superati:', assessment['all_diagnostic_gates_passed'])'''),
        md('''## Dati: 6.539 rendimenti giornalieri per strategia

Baseline Watchtower 113 e 108. La barra extra del 2 gennaio 2026, oltre il
termine dichiarato del run, viene esclusa. Commissioni configurate alpaca ma
commissioni registrate sui trade pari a zero: il rendimento segue la contabilità
di questi backtest, senza certificare costi reali del broker. I file originali
sono congelati con hash; non è stato lanciato un nuovo backtest economico.'''),
        code('''quality = pd.DataFrame(json.loads((ROOT/'results/data_quality.json').read_text()))
display(quality[['strategy','baseline_id','start','end','rows','total_trades_in_window',
                 'zero_return_days','recorded_trade_commissions']])
display(Image(filename=str(ROOT/'figures/01_native_returns.png')))'''),
        md('''## Risultati controllati: ampiezza e ordine

Stessi blocchi reali per entrambi i metodi. C5-Q6 usa sei fasce definite dai
quantili iniziali e riusa le statistiche precedenti. F1 usa valori continui,
cumulato, dispersione, estremi, drawdown, sequenze di perdite e prodotti a ritardo.

Nelle tabelle **44,35% significa circa 44 gruppi segnalati ogni 100 gruppi di 20
sedute** nello scenario dichiarato. σ vale 1,58% per AH e 3,53% per FlatComposite,
in rendimento log giornaliero: gli stress non hanno uguale ampiezza economica
fra le strategie. Le alterazioni non sono risultati di strategie alternative.
Gli IC sono condizionati alla stessa storia finita, fra 50 banche Monte Carlo.'''),
        code('''display(Image(filename=str(ROOT/'figures/02_controlled_changes.png')))
joint = controlled[controlled.window.eq('joint')]
grid = joint.pivot(index='scenario', columns=['strategy','method'], values='rate')
display((grid*100).round(2).rename_axis('Scenario · gruppi segnalati (%)'))
display(pd.read_csv(ROOT/'results/diagnostics.csv').round(4))'''),
        md('''### Che cosa mostra il confronto

Il candidato categoriale distingue l'ordine, ma una perdita che cresce restando
nella stessa fascia lascia invariato il punteggio. F1 conserva l'ampiezza e
riconosce lo stress estremo, ma manca il criterio sulla dispersione e perde
sensibilità su alcuni riordini. La permutazione casuale non dimostra capacità
generale di riconoscere la rimozione della dipendenza finanziaria.

Il minimo p-value dipende dai blocchi disponibili: anche un punteggio che cresce
può arrivare allo stesso limite inferiore. Un 100% osservato in uno stress non
stabilisce probabilità teorica di riconoscimento pari a uno.'''),
        code('''cases = ['unchanged','mean_minus_0.25sd','dispersion_x2','sorted_same_values']
specific = controlled[(controlled.strategy=='OvernightAH') & (controlled.method=='F1_continuous')
                      & controlled.scenario.isin(cases)]
window_table = specific.pivot(index='scenario', columns='window', values='rate')
display((window_table[['3','5','10','15','20','joint']]*100).round(2)
        .rename_axis('OvernightAH · gruppi segnalati (%)'))
gates = pd.DataFrame(assessment['gates'])
display(gates[['strategy','criterion','rate','passed']].round(4))'''),
        md('''## Cronologia reale: sensibilità al riferimento

La percentuale indica le sedute con un segnale sulle cinque finestre calibrate
insieme. Finestre sovrapposte non sono episodi indipendenti. Non conosciamo gli
anni di processo invariato: questi tassi non validano la soglia del 5%.
Una baseline mobile può ridurre segnali assorbendo un cambiamento persistente.'''),
        code('''display(Image(filename=str(ROOT/'figures/03_historical_reference_sensitivity.png')))
recent = historical[(historical.window=='joint') & historical.year.isin([2024,2025])]
display(recent[['strategy','year','method','alarms','observations','rate']].round(4))
ui = historical[(historical.strategy=='OvernightAH') & (historical.year==2025)
                & (historical.window=='20')]
display(ui[['method','alarms','observations','rate']].round(4))'''),
        md('''### Il dato UI e il rendimento del portafoglio rispondono a domande diverse

La ricostruzione UI utilizza media e numerosità dei trade chiusi nelle ultime N
sedute, con riferimento precedente al 2019. F1 usa una sequenza di rendimenti NAV
giornalieri. La differenza fra i loro tassi di segnale non misura accuratezza.
Il gain medio/trade può restare descrittivo; la compatibilità temporale richiede
una definizione separata e coerente della variabile monitorata.'''),
        md('''## Monitoraggio ripetuto: capacità della banca storica

113 gruppi di 20 sedute per il controllo singolo. Per proteggere un intero periodo
di 20 controlli quotidiani, restano 38 periodi: rango minimo 2,56%. Per 100
controlli restano 16 periodi: minimo 5,88%, quindi una soglia del 5% non è stimabile
con questo schema. Lo zero allarmi nei casi non stimabili non indica successo.
Non è dimostrata una protezione perpetua o rinnovabile automaticamente.'''),
        code('''display(pd.read_csv(ROOT/'results/monitoring_capacity.csv').round(4))
periods = pd.read_csv(ROOT/'results/monitoring_periods.csv')
display(periods.groupby(['strategy','horizon']).agg(
    periods=('period_alarm','size'), any_daily_alarm=('any_daily_alarm','mean'),
    period_alarm=('period_alarm','mean'), estimable=('estimable','first')).round(4))
validation = json.loads((ROOT/'results/validation.json').read_text())
print('Verifica indipendente superata:', validation['passed'])
print('Trade ricostruiti:', validation['trade_rows_independently_aggregated'])
print('Righe controllate:', validation['raw_controlled_rows_checked'])
print('Celle di rango indipendenti campionate:', validation['independent_rank_count_cells'])'''),
        md('''## Decisione

**Adattabilità parziale, applicazione non pronta.** Si riusano il confronto per
blocchi, la separazione cronologica e la calibrazione congiunta. Si deve superare
la rappresentazione solo categoriale e migliorare la sensibilità finanziaria
alla dispersione e alla dipendenza. Il candidato continuo qui provato non passa
tutti i criteri: non viene promosso.

Prima dell'applicazione vanno definite serie monitorata, baseline e orizzonte;
serve una revisione statistica mirata e una verifica su nuove osservazioni o
scenari finanziari dipendenti separati dallo sviluppo. Nessun risultato permette
di attribuire un'anomalia alla perdita di edge o di attivare uno stop automatico.

Il controllo indipendente riguarda aggregazioni, formule scalari, normalizzazione
completa, conteggi di rango campionati, aritmetica delle righe e hash. Non dimostra
stazionarietà del processo. I dettagli sono in [conclusioni.md](conclusioni.md),
[protocol.md](protocol.md) e [sources.md](sources.md).'''),
    ]
    notebook.metadata = {'kernelspec': {'display_name': 'Python (statistical study)',
                                      'language': 'python', 'name': 'statistical-study'},
                         'language_info': {'name': 'python'}}
    nbf.validate(notebook)
    nbf.write(notebook, ROOT / 'studio.ipynb')


if __name__ == '__main__':
    main()
