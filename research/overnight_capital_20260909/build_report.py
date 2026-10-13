"""Build the canonical, source-backed report payload from validated results."""
from pathlib import Path
import json
import numpy as np
import pandas as pd
import sqlite3

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'out/overnight_ah_capital_study_20260909'
LABELS = {'legacy': 'OvernightAH', 'composite': 'Flat composite'}


def money(x):
    if x >= 1e6:
        return f'${x / 1e6:g}M'
    return f'${x / 1000:g}k'


def records(df):
    return json.loads(df.to_json(orient='records'))


def main():
    all_results = pd.read_csv(OUT / 'summary.csv')
    primary = all_results[all_results.case.str.startswith('capital_band20_')].copy()
    # The $30M stress cases cannot return to C when the threshold is crossed:
    # stale/open orders defer withdrawals for years. They are diagnostics,
    # not valid observations of the requested policy.
    s = primary[primary.capital <= 10_000_000].copy()
    assert len(s) >= 16, 'The primary sweep through $10M is not complete.'
    s = s.sort_values(['capital', 'strategy'])
    s['strategy_label'] = s.strategy.map(LABELS)
    s['capital_label'] = s.capital.map(money)
    annual = pd.read_csv(OUT / 'annual.csv')
    annual = annual[annual.complete & annual.case.isin(s.case)].copy()
    audit = pd.read_csv(OUT / 'audit_summary.csv')
    restart = all_results[all_results.case.str.startswith('capital_restart2013_band20_')].copy()
    restart['strategy_label'] = restart.strategy.map(LABELS)
    choices = []
    selected = []
    for strategy, group in s.groupby('strategy'):
        for floor in [1.0, 1.25, 1.5, 1.75, 2.0]:
            eligible = group[group.sharpe >= floor]
            if eligible.empty:
                choices.append(dict(strategy=LABELS[strategy], min_sharpe=floor,
                                    capital=None, yield_on_capital=None, payout=None,
                                    sharpe=None, near_optimal_range='Nessun livello eleggibile'))
                continue
            best = eligible.loc[eligible.avg_annual_withdrawal_yield.idxmax()]
            near = eligible[eligible.avg_annual_withdrawal_yield >= .95 * best.avg_annual_withdrawal_yield]
            choices.append(dict(strategy=LABELS[strategy], min_sharpe=floor,
                                capital=best.capital, yield_on_capital=best.avg_annual_withdrawal_yield,
                                payout=best.avg_annual_withdrawal, sharpe=best.sharpe,
                                near_optimal_range=', '.join(money(x) for x in near.capital)))
            if floor == 1.5:
                selected.append(best)
    pd.DataFrame(choices).to_csv(OUT / 'sweet_spots.csv', index=False)
    # Paired three-year circular block bootstrap: sample sensitivity, not a forecast.
    rng = np.random.default_rng(20260909)
    sensitivity = []
    for best in selected:
        a = annual[annual.strategy == best.strategy].pivot(index='year', columns='capital', values='withdrawal_yield').sort_index()
        n = len(a)
        starts = rng.integers(0, n, size=(5000, int(np.ceil(n / 3))))
        ix = ((starts[..., None] + np.arange(3)) % n).reshape(5000, -1)[:, :n]
        sampled = a.to_numpy()[ix].mean(axis=1)
        best_index = list(a.columns).index(best.capital)
        for j, capital in enumerate(a.columns):
            difference = sampled[:, best_index] - sampled[:, j]
            sensitivity.append(dict(strategy=best.strategy, selected_capital=best.capital,
                                    comparison_capital=capital,
                                    difference_p025=float(np.quantile(difference, .025)),
                                    difference_p975=float(np.quantile(difference, .975)),
                                    positive_difference_share=float(np.mean(difference > 0))))
    pd.DataFrame(sensitivity).to_csv(OUT / 'paired_year_sensitivity.csv', index=False)

    title = 'Capitale operativo e prelievi Overnight AH'
    annual_selected = annual[annual.case.isin([x.case for x in selected])].copy()
    annual_selected['series'] = annual_selected.apply(lambda x: f'{LABELS[x.strategy]} {money(x.capital)}', axis=1)
    annual_selected['year_label'] = annual_selected.year.astype(str)
    # Materialize the exact report datasets and read them back with the SQL
    # exposed in source metadata, so report provenance is executable rather
    # than a prose description masquerading as a query.
    db = sqlite3.connect(OUT / 'report_data.sqlite')
    datasets = dict(capital=s, audit=audit, choices=pd.DataFrame(choices),
                    annual_selected=annual_selected, restart=restart)
    sql = {name: f'SELECT * FROM {name}' for name in datasets}
    for name, frame in datasets.items():
        frame.to_sql(name, db, if_exists='replace', index=False)
        datasets[name] = pd.read_sql_query(sql[name], db)
    db.close()

    def sql_source(id, label, table):
        return dict(id=id, label=label, path='report_data.sqlite', query=dict(
            engine='SQLite', language='sql', sql=sql[table],
            description=f'Lettura delle righe verificate della tabella {table}.',
            tables_used=[table]))

    sources = [
        sql_source('sweep', 'Backtest con prelievi: metriche verificate', 'capital'),
        sql_source('audit', 'Confronto con le simulazioni originali', 'audit'),
        sql_source('annual', 'Profitti e prelievi per anno civile', 'annual_selected'),
        sql_source('choices', 'Scelta per soglia di Sharpe e sensibilità', 'choices'),
        sql_source('restart', 'Riavvii indipendenti dal 2013', 'restart'),
        dict(id='code', label='Parametri, codice e impronte dei dati', path='audit_manifest.json'),
        dict(id='gips', label='GIPS: rendimenti corretti per flussi esterni', href='https://www.gipsstandards.org/standards/gips-standards-for-firms/gips-standards-handbook-for-firms/'),
    ]
    blocks, charts, tables = [], [], []

    def md(id, body, source=None):
        block = dict(id=id, type='markdown', body=body)
        if source:
            block['sourceId'] = source
        blocks.append(block)

    def table(id, title, dataset, columns, sort, source):
        tables.append(dict(id=id, title=title, dataset=dataset, sourceId=source,
                           defaultSort=dict(field=sort, direction='asc'), columns=columns))
        blocks.append(dict(id=f'{id}_block', type='table', tableId=id, layout='full'))

    def line(id, title, field, ylabel, fmt='number', refs=None):
        chart = dict(id=id, title=title, subtitle='Livelli discreti di capitale; prelievo sopra 1,2 × C e ritorno a C. Nessun versamento.',
                     type='line', dataset='capital', sourceId='sweep', layout='full',
                     encodings=dict(x=dict(field='capital_label', type='ordinal', label='Capitale operativo C ($), livelli testati'),
                                    y=dict(field=field, type='quantitative', label=ylabel, format=fmt),
                                    color=dict(field='strategy_label', type='nominal', label='Strategia')),
                     palette=dict(kind='categorical'), legend=dict(position='bottom'), valueFormat=fmt)
        if refs:
            chart['referenceLines'] = refs
        charts.append(chart)
        blocks.append(dict(id=f'{id}_block', type='chart', chartId=id, layout='full'))

    md('title', '# ' + title)
    summaries = []
    for best in selected:
        summaries.append(f"**{LABELS[best.strategy]}:** con Sharpe minimo illustrativo 1,5, il massimo reddito prelevato per capitale nella griglia è a **{money(best.capital)}**: {best.avg_annual_withdrawal_yield:.1%}/anno, circa ${best.avg_annual_withdrawal:,.0f}/anno prelevati, Sharpe {best.sharpe:.3f}.")
    md('summary', '## Executive Summary\n\n' + '\n\n'.join(summaries) +
       '\n\n**Sono medie storiche, non un reddito garantito.** La tabella delle soglie e i sottoperiodi servono a valutare quanto sia stabile la scelta; il massimo puntuale non dimostra un ottimo futuro.', 'sweep')
    md('audit_text', '## Il codice attuale non riproduce tutto il comportamento storico\n\n'
       'Il riferimento è 50.000 $, senza costi, dal 3 gennaio 2000 al 14 agosto 2026. '
       'Il codice di agosto sui dati attuali riproduce quasi esattamente il risultato salvato. '
       'Il cooldown attuale usa invece la media dei rendimenti overnight dell’universo, anziché il PnL delle posizioni chiuse: la stessa soglia 3% non identifica lo stesso evento. '
       'Inoltre il parametro nuovo `entry_signal_lag_bars=1` legge il giorno precedente; un valore negativo viene limitato a zero. '
       'La griglia usa il codice attuale, lag 1 e gli altri parametri dello studio, con costi.', 'audit')
    table('audit_table', 'Confronto della base codice — capitale iniziale $50k', 'audit', [
        dict(field='label', label='Configurazione', type='text'),
        dict(field='final_equity', label='Capitale finale ($)', format='currency'),
        dict(field='sharpe', label='Sharpe giornaliero', format='number'),
        dict(field='max_dd', label='Max drawdown', format='percent'),
    ], 'label', 'audit')

    best_yield = s.loc[s.avg_annual_yield.idxmax()]
    md('yield_text', '## Il rendimento per dollaro va confrontato con la capacità\n\n'
       f'Il massimo rendimento economico medio nella griglia è {best_yield.avg_annual_yield:.1%} a {money(best_yield.capital)} ({LABELS[best_yield.strategy]}), prima di imporre una soglia di Sharpe. '
       'Ogni punto è un backtest completo con un diverso capitale: non è una riscalatura della stessa curva. '
       'Il rendimento economico è la media, sui 26 anni completi 2000–2025, di (prelievi + variazione del conto) / C. '
       'Il 2026 parziale resta nei risultati di percorso e nello Sharpe, ma non nella media degli anni completi.', 'sweep')
    line('yield', 'Rendimento economico medio annuo per capitale', 'avg_annual_yield', 'Profitto annuo / C', 'percent')
    max_income = s.loc[s.avg_annual_withdrawal.idxmax()]
    md('dollars_text', '## Più reddito in dollari può richiedere più capitale per ogni dollaro guadagnato\n\n'
       f'Il maggiore prelievo medio assoluto osservato è ${max_income.avg_annual_withdrawal:,.0f}/anno a {money(max_income.capital)} ({LABELS[max_income.strategy]}). '
       'Il grafico mostra il profitto economico; i prelievi effettivi sono riportati nella tabella. '
       'Massimizzare il profitto in dollari e massimizzare il reddito per capitale sono obiettivi distinti.', 'sweep')
    line('dollars', 'Profitto economico medio annuo in dollari', 'avg_annual_profit', 'Profitto annuo ($)', 'currency')
    md('sharpe_text', '## Una soglia di Sharpe cambia quali capitali sono eleggibili\n\n'
       'Lo Sharpe usa la media e la deviazione standard campionaria dei rendimenti giornalieri corretti per i prelievi, annualizzate con √252 e tasso privo di rischio zero. '
       'Non si usa lo Sharpe annuale stampato dall’analyzer standard, né la serie grezza che considera i prelievi una riduzione del patrimonio. '
       'La linea 1,5 è una soglia illustrativa di confronto, non una regola universale.', 'sweep')
    line('sharpe', 'Sharpe per capitale operativo — gennaio 2000–agosto 2026', 'sharpe', 'Sharpe',
         refs=[dict(axis='y', value=1.5, label='Soglia illustrativa 1,5', color='neutral', lineStyle='dashed')])
    md('lookup_text', '## Il confronto completo include reddito, rischio e anni deboli\n\n'
       'Il decimo percentile descrive un anno storicamente debole; non è una previsione probabilistica. '
       'Il minimo conto / C mostra quanto il conto si è ridotto rispetto al capitale operativo, anche dopo avere già erogato prelievi.', 'sweep')
    table('lookup', 'Metriche per livello di capitale', 'capital', [
        dict(field='strategy_label', label='Strategia', type='text'),
        dict(field='capital', label='Capitale C ($)', format='currency'),
        dict(field='avg_annual_yield', label='Profitto annuo / C', format='percent'),
        dict(field='avg_annual_profit', label='Profitto annuo ($)', format='currency'),
        dict(field='avg_annual_withdrawal', label='Prelievo annuo ($)', format='currency'),
        dict(field='sharpe', label='Sharpe', format='number'),
        dict(field='p10_annual_yield', label='P10 annuo / C', format='percent'),
        dict(field='min_equity_ratio', label='Minimo conto / C', format='percent'),
        dict(field='max_days_without_withdrawal', label='Max giorni senza prelievi', format='number'),
    ], 'capital', 'sweep')
    md('choices_text', '## Lo sweet spot è una fascia da scegliere sotto un vincolo\n\n'
       'Per ciascuna soglia si massimizza il prelievo medio annuo / C tra i livelli eleggibili. '
       'La fascia elenca i capitali che conservano almeno il 95% del rendimento massimo eleggibile. '
       'Questo evita di attribuire troppo significato a differenze minime tra singoli punti. '
       'La risimulazione di blocchi di tre anni, salvata tra gli allegati, misura la sensibilità della graduatoria agli anni osservati; non corregge l’ottimizzazione ex post della strategia.', 'choices')
    table('choices', 'Capitale selezionato per soglia minima di Sharpe', 'choices', [
        dict(field='strategy', label='Strategia', type='text'),
        dict(field='min_sharpe', label='Sharpe minimo', format='number'),
        dict(field='capital', label='Capitale migliore ($)', format='currency'),
        dict(field='yield_on_capital', label='Prelievo annuo / C', format='percent'),
        dict(field='payout', label='Prelievo annuo ($)', format='currency'),
        dict(field='sharpe', label='Sharpe osservato', format='number'),
        dict(field='near_optimal_range', label='Livelli entro 5% dal massimo', type='text'),
    ], 'min_sharpe', 'choices')

    md('years_text', '## La media annua non equivale a un flusso regolare\n\n'
       'Il grafico mostra i prelievi realizzati in ciascun anno dai capitali selezionati con soglia Sharpe 1,5. '
       'Dopo le perdite il prelievo resta sospeso finché il conto non supera nuovamente 1,2 × C: non viene aggiunto denaro esterno.', 'annual')
    charts.append(dict(id='annual_payout', title='Prelievi annuali / capitale operativo — 2000–2025',
                       type='bar', dataset='annual_selected', sourceId='annual', layout='full',
                       encodings=dict(x=dict(field='year_label', type='ordinal', label='Anno'),
                                      y=dict(field='withdrawal_yield', type='quantitative', label='Prelievi / C', format='percent'),
                                      color=dict(field='series', type='nominal', label='Strategia e capitale')),
                       palette=dict(kind='categorical'), legend=dict(position='bottom'), valueFormat='percent'))
    blocks.append(dict(id='annual_payout_block', type='chart', chartId='annual_payout', layout='full'))
    if len(restart):
        md('restart_text', '## I riavvii dal 2013 verificano la dipendenza dalla partenza\n\n'
           'Questi run partono nuovamente con C nel 2013, conservando i dati precedenti soltanto per gli indicatori. '
           'Le medie annuali coprono il 2013–2025; non sono semplici ritagli delle curve partite nel 2000. '
           'Sono controlli di sensibilità al regime, non prove fuori campione della selezione dei titoli o dei pesi composite.', 'restart')
        table('restart', 'Riavvii dal 2013 con fascia +20%', 'restart', [
            dict(field='strategy_label', label='Strategia', type='text'),
            dict(field='capital', label='Capitale ($)', format='currency'),
            dict(field='avg_annual_yield', label='Profitto annuo / C', format='percent'),
            dict(field='avg_annual_withdrawal', label='Prelievo annuo ($)', format='currency'),
            dict(field='sharpe', label='Sharpe', format='number'),
            dict(field='min_equity_ratio', label='Minimo conto / C', format='percent'),
        ], 'capital', 'restart')
    md('next_steps', '## Usare la fascia efficiente prima di fissare un reddito\n\n'
       'La scelta operativa va fatta nella fascia che conserva rendimento per capitale e Sharpe, confrontando il reddito in dollari con gli anni senza prelievi. '
       'Una differenza minima di rendimento medio non giustifica da sola un capitale preciso. '
       'Restano da fissare il reddito minimo desiderato e la perdita temporanea sostenibile; non sono stati trasformati in soglie arbitrarie nello studio.')
    md('caveats', '## Assunzioni che incidono sull’interpretazione\n\n'
       '- **Backtest storico:** universo e pesi composite sono frutto di ricerca sul passato; le medie non stimano senza distorsioni un reddito futuro.\n'
       '- **Stesso modello dello studio:** leva 2×, cap ordini 1% dei volumi, da 3 a 50 posizioni; costi Alpaca del codice e margine fisso 4,75%. Nessuna tassazione, remunerazione dei prelievi o simulazione aggiuntiva di impatto di mercato.\n'
       '- **Soglia +20%:** si preleva tutto l’eccesso sopra C al primo controllo giornaliero utile a posizioni chiuse, prima dei nuovi ingressi. Un salto può superare la soglia; nessun prelievo finanzia posizioni a margine. Nessun versamento o reddito mensile fisso.\n'
       '- **Capitale ed esecuzione:** il cap implementato non è una verifica della liquidità effettivamente eseguibile nelle aste. Il ramo hedge eredita il suo sizing dal codice corrente.\n'
       '- **Stress 30 milioni escluso:** a quel livello il conto ha superato 1,2 × C mentre ordini o posizioni impedivano il prelievo per lunghi intervalli (fino a 2.413 giorni segnalati; equity fino a 9,4 × C). Il caso viola la policy richiesta e non entra nei grafici o nello sweet spot.\n'
       '- **Versione:** cooldown 3% dello studio sul segnale attuale, non il vecchio segnale di PnL e non la soglia 1% del profilo development attuale. I parametri operativi dei profili non sono stati modificati.\n'
       '- **Dati:** 103 simboli congelati, 568.342 righe OHLCV nel periodo, senza duplicati di data, OHLCV mancanti, close non positivi o volumi negativi. La copia esatta del dataset originario non era disponibile: il codice archiviato di agosto è un controllo vicino, non una riproduzione certificata dell’ambiente originario.', 'code')
    md('flow_method', '### Contabilità dei flussi\n\n'
       'Per ogni giorno, profitto = conto finale + prelievo − conto precedente; rendimento = profitto / conto precedente. '
       'Nel runner i prelievi avvengono dopo i fill e prima del dimensionamento dei nuovi ordini. '
       'Il controllo senza prelievi riproduce i rendimenti standard entro 2,1 × 10⁻¹⁶. '
       'La separazione dei flussi esterni è coerente con il principio dei rendimenti time-weighted descritto nei [GIPS](https://www.gipsstandards.org/standards/gips-standards-for-firms/gips-standards-handbook-for-firms/).')
    artifact = dict(surface='report', manifest=dict(version=1, surface='report', title=title,
                    description='Confronto della base codice, capacità del capitale e prelievi a soglia +20%.',
                    blocks=blocks, charts=charts, tables=tables, sources=sources),
                    snapshot=dict(version=1, status='ready', datasets={
                        name: records(frame) for name, frame in datasets.items()
                    }), sources=sources)
    (OUT / 'artifact.json').write_text(json.dumps(artifact, ensure_ascii=False, indent=2))
    print('Artifact:', OUT / 'artifact.json')


if __name__ == '__main__':
    main()
