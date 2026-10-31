"""Notebook-native Matplotlib figures backed by the frozen CSV evidence."""
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import PercentFormatter

ROOT = Path(__file__).resolve().parent
COLORS = dict(mean_iid='#B45309', ensemble5_blocks='#2563EB', null='#6B7280',
              iid='#2563EB', sticky_09='#B45309', sticky_06='#6B7280')
NULL_LABELS = {'iid': 'Indipendente', 'sticky_06': 'Magnetico sticky',
               'cycle_forward': 'Ciclo direzionale', 'lag2': 'Memoria a due passi'}


def style():
    plt.rcParams.update({'font.size': 11, 'axes.titlesize': 13, 'axes.labelsize': 11,
                         'figure.facecolor': 'white', 'axes.facecolor': 'white',
                         'axes.spines.top': False, 'axes.spines.right': False,
                         'axes.edgecolor': '#374151', 'text.color': '#111827',
                         'axes.labelcolor': '#111827', 'xtick.color': '#374151',
                         'ytick.color': '#374151', 'savefig.bbox': 'tight'})


def save(fig, name):
    folder = ROOT / 'figures'
    folder.mkdir(exist_ok=True)
    fig.savefig(folder / f'{name}.png', dpi=150)
    fig.savefig(folder / f'{name}.svg')
    return fig


def null_rates():
    style()
    data = pd.read_csv(ROOT / 'results/rejection_rates.csv')
    fig, ax = plt.subplots(figsize=(10, 5.3))
    order = list(NULL_LABELS)
    for offset, method, label, marker in [(-.09, 'mean_iid', 'Media con indipendenza', 's'),
                                           (.09, 'ensemble5_blocks', 'Score temporale, calibrazione a blocchi', 'o')]:
        rows = data[(data.is_null) & (data.window == 20) & (data.method == method)].set_index('null').loc[order]
        ax.errorbar(np.arange(4) + offset, rows.rate,
                    yerr=[rows.rate - rows.ci95_low, rows.ci95_high - rows.rate],
                    fmt=marker, capsize=4, color=COLORS[method], markersize=7, label=label)
    ax.axhline(.05, color='#374151', linestyle='--', label='Soglia nominale 5%')
    ax.set(xticks=np.arange(4), xticklabels=[NULL_LABELS[x] for x in order], ylim=(0, .46),
           ylabel='Finestre segnalate con processo invariato',
           title='Falsi allarmi su finestre di 20 lanci')
    ax.yaxis.set_major_formatter(PercentFormatter(1))
    ax.grid(axis='y', alpha=.15)
    ax.legend(loc='upper left', frameon=False)
    fig.text(.08, .02, '10.000 episodi indipendenti per processo. IC Wilson 95%, condizionati alla baseline simulata.\nLa calibrazione a blocchi è empirica: nessuna garanzia esatta universale al 5%.', fontsize=10)
    fig.tight_layout(rect=(0, .08, 1, 1))
    return save(fig, '01_false_alarms')


def power():
    style()
    data = pd.read_csv(ROOT / 'results/rejection_rates.csv')
    fig, ax = plt.subplots(figsize=(9.7, 5.5))
    for scenario, label, marker in [('iid', 'Magnetismo rimosso', 'o'),
                                    ('sticky_09', 'Magnetismo aumentato', 's'),
                                    ('sticky_06', 'Nessun cambiamento', '^')]:
        rows = data[(data['null'] == 'sticky_06') & (data.scenario == scenario) & (data.method == 'ensemble5_blocks')].sort_values('window')
        ax.errorbar(rows.window, rows.rate,
                    yerr=[rows.rate - rows.ci95_low, rows.ci95_high - rows.rate],
                    fmt=marker + '-', capsize=3, color=COLORS[scenario], label=label)
    ax.axhline(.05, color='#374151', linestyle='--', linewidth=1)
    ax.set(xticks=[3, 5, 10, 15, 20], ylim=(0, 1.04), xlabel='Lanci consecutivi nella finestra',
           ylabel='Finestre segnalate', title='La forza del cambiamento e la lunghezza della finestra contano')
    ax.yaxis.set_major_formatter(PercentFormatter(1))
    ax.grid(axis='y', alpha=.15)
    ax.legend(frameon=False, loc='upper left')
    fig.text(.08, .02, 'Baseline sticky: P(stessa faccia al prossimo lancio) = 2/3. Marginali sempre uniformi.\nScore a cinque componenti; 10.000 episodi per scenario, IC Wilson 95% condizionati alla baseline.', fontsize=10)
    fig.tight_layout(rect=(0, .08, 1, 1))
    return save(fig, '02_power_by_length')


def repeated_monitoring():
    style()
    data = pd.read_csv(ROOT / 'results/monitoring.csv')
    fig, ax = plt.subplots(figsize=(10, 5.8))
    order = list(NULL_LABELS)
    for offset, method, label, color in [(-.19, 'five_windows_unadjusted', 'Cinque finestre senza correzione', '#B45309'),
                                        (.19, 'five_windows_jointly_calibrated', 'Cinque finestre calibrate allo stesso istante', '#2563EB')]:
        rows = data[data.method == method].set_index('null').loc[order]
        ax.bar(np.arange(4) + offset, rows.any_alarm_rate, width=.34, color=color, label=label)
        ax.errorbar(np.arange(4) + offset, rows.any_alarm_rate,
                    yerr=[rows.any_alarm_rate - rows.ci95_low, rows.ci95_high - rows.any_alarm_rate],
                    fmt='none', capsize=3, color='#111827')
    ax.set(xticks=np.arange(4), xticklabels=[NULL_LABELS[x] for x in order], ylim=(0, 1.08),
           ylabel='Traiettorie con almeno un falso allarme',
           title='Ripetere il controllo per 100 istanti accumula falsi allarmi')
    ax.yaxis.set_major_formatter(PercentFormatter(1))
    ax.set_title('')
    fig.suptitle('Ripetere il controllo per 100 istanti accumula falsi allarmi', y=.99, fontsize=14)
    handles, labels = ax.get_legend_handles_labels()
    fig.legend(handles, labels, frameon=False, loc='upper center', bbox_to_anchor=(.5, .94), fontsize=10)
    fig.text(.08, .02, '1.000 traiettorie per processo; IC Wilson 95%. Nessun processo cambia.\nIl 5% è una soglia puntuale: non è una garanzia sul periodo di 100 controlli.', fontsize=10)
    fig.tight_layout(rect=(0, .08, 1, .80))
    return save(fig, '03_repeated_monitoring')


def bootstrap():
    style()
    data = pd.read_csv(ROOT / 'results/bootstrap_sensitivity.csv')
    selected = data[data.expected_block_length.notna()]
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.8))
    axes[0].plot(selected.expected_block_length, selected.sd_mean, 'o-', color='#2563EB')
    oracle = data[data.method == 'oracle_fresh_paths'].iloc[0]
    axes[0].axhline(oracle.sd_mean, color='#374151', linestyle='--', label='100.000 blocchi veri simulati')
    axes[0].set(ylabel='Deviazione standard della media di 20 lanci', title='Dispersione stimata')
    axes[0].legend(frameon=False, fontsize=9)
    axes[1].plot(selected.expected_block_length, selected.null_rejection_rate, 's-', color='#B45309')
    axes[1].axhline(.05, color='#374151', linestyle='--')
    axes[1].set(ylabel='Falsi allarmi della regione centrale empirica', title='Calibrazione della media')
    axes[1].yaxis.set_major_formatter(PercentFormatter(1))
    for ax in axes:
        ax.set(xlabel='Lunghezza media del blocco (1 = iid)', xscale='log', xticks=[1, 2, 5, 10, 20, 50])
        ax.set_xticklabels(['1', '2', '5', '10', '20', '50'])
        ax.grid(axis='y', alpha=.15)
    fig.suptitle('Il ricampionamento a blocchi dipende dalla lunghezza scelta', fontsize=14)
    fig.text(.07, .02, 'Baseline sticky invariata. 20.000 ricampionamenti per scelta; valutazione su 100.000 blocchi indipendenti.\nRisultato di uno scenario: 50 non è una lunghezza universalmente raccomandata.', fontsize=10)
    fig.tight_layout(rect=(0, .09, 1, .95))
    return save(fig, '04_bootstrap_sensitivity')
