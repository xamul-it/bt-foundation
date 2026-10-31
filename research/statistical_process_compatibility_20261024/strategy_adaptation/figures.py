"""Portable research figures from reviewed strategy evidence."""
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parent
COLORS = {'OvernightAH': '#175e91', 'FlatComposite': '#b35b13',
          'C5_Q6': '#68737d', 'F1_continuous': '#175e91',
          'F1_continuous_fixed': '#175e91', 'F1_continuous_rolling5y': '#b35b13',
          'C5_Q6_fixed': '#68737d'}


def save(fig, filename):
    for extension in ['png', 'svg']:
        fig.savefig(ROOT / 'figures' / f'{filename}.{extension}', dpi=150, bbox_inches='tight')
    plt.close(fig)


def main():
    plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 11,
                         'axes.spines.top': False, 'axes.spines.right': False,
                         'axes.grid': True, 'grid.alpha': .17, 'axes.axisbelow': True})
    annual_rows = []
    for name in ['OvernightAH', 'FlatComposite']:
        data = pd.read_csv(ROOT / 'results' / f'{name}_daily.csv', index_col=0, parse_dates=True)
        annual = data.groupby(data.index.year)['return'].agg(['mean', 'std', 'size'])
        annual['zero_rate'] = (data['return'] == 0).groupby(data.index.year).mean()
        annual['strategy'] = name
        annual_rows.append(annual.reset_index(names='year'))
    annual = pd.concat(annual_rows, ignore_index=True)
    annual.to_csv(ROOT / 'results/annual_descriptive.csv', index=False)
    fig, axes = plt.subplots(2, 1, figsize=(11.5, 7), sharex=True, layout='constrained')
    for name, group in annual.groupby('strategy'):
        axes[0].plot(group.year, group['std']*100, label=name, color=COLORS[name], marker='o', ms=3)
        axes[1].plot(group.year, group.zero_rate*100, label=name, color=COLORS[name], marker='s', ms=3)
    axes[0].set_title('Rendimenti nativi 2000–2025: dispersione e giorni a zero')
    axes[0].set_ylabel('Deviazione standard\ngiornaliera (%)')
    axes[0].set_ylim(bottom=0)
    axes[0].legend(loc='upper right', frameon=False)
    axes[1].set_ylabel('Sedute con rendimento\nNAV pari a zero (%)')
    axes[1].set_ylim(0, 55)
    axes[1].set_xticks([2000, 2005, 2010, 2015, 2020, 2025])
    axes[1].set_xlim(2000, 2025)
    fig.supxlabel('Anno · backtest delle baseline 113 e 108; esposizione e configurazioni differenti', fontsize=10)
    save(fig, '01_native_returns')

    controlled = pd.read_csv(ROOT / 'results/controlled_summary.csv', dtype={'window': str})
    scenarios = ['unchanged', 'mean_minus_0.25sd', 'dispersion_x2', 'last_loss_6sd', 'sorted_same_values']
    labels = ['Blocco invariato', 'Rendimento ridotto\ndi 0,25 σ al giorno',
              'Dispersione ×2', 'Ultima perdita ≥6 σ', 'Stessi 20 valori\nordinati in crescita']
    fig, axes = plt.subplots(1, 2, figsize=(13.4, 6.1), sharex=True, sharey=True)
    for ax, name in zip(axes, ['OvernightAH', 'FlatComposite']):
        for offset, kind, label in [(-.16, 'C5_Q6', 'Candidato a sei categorie'), (.16, 'F1_continuous', 'Adattamento continuo')]:
            data = controlled[(controlled.strategy == name) & (controlled.method == kind) & (controlled.window == 'joint')].set_index('scenario').loc[scenarios]
            y = np.arange(len(data)) + offset
            ax.barh(y, data.rate*100, height=.29, color=COLORS[kind], label=label)
            ax.errorbar(data.rate*100, y, xerr=np.array([data.rate-data.ci95_low, data.ci95_high-data.rate])*100,
                        fmt='none', ecolor='#202b33', capsize=3, lw=.8)
            for position, rate, upper in zip(y, data.rate, data.ci95_high):
                ax.text(upper*100+1.4, position, f'{rate*100:.1f}%', va='center', fontsize=9)
        ax.set_title(name)
        ax.set_xlim(0, 114)
        ax.set_xticks([0, 25, 50, 75, 100])
        ax.xaxis.set_major_formatter(PercentFormatter(100))
        ax.set_xlabel('Gruppi segnalati (%)')
        ax.set_yticks(np.arange(len(labels)), labels)
        ax.axvline(5, color='#3d4145', lw=.8, ls='--')
    axes[0].invert_yaxis()
    handles, legend_labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, legend_labels, loc='upper center', ncol=2, frameon=False, bbox_to_anchor=(.55, .93))
    fig.suptitle('Esperimenti su blocchi di 20 rendimenti finanziari: cosa riconoscono i candidati', y=1.005)
    fig.text(.02, .01, 'Fonte: blocchi 2010–2018; massimo calibrato sulle finestre 3/5/10/15/20. 50 banche × 2.000 gruppi.\n'
             'σ del periodo iniziale: 1,58% OvernightAH, 3,53% FlatComposite (rendimenti log). IC 95% Monte Carlo condizionati alla storia.', fontsize=9)
    fig.subplots_adjust(left=.23, right=.98, bottom=.18, top=.83, wspace=.10)
    save(fig, '02_controlled_changes')

    history = pd.read_csv(ROOT / 'results/historical_annual.csv', dtype={'window': str})
    fig, axes = plt.subplots(1, 2, figsize=(12, 5.1), sharey=True, layout='constrained')
    for ax, name in zip(axes, ['OvernightAH', 'FlatComposite']):
        for kind, label, marker in [('C5_Q6_fixed', 'Sei categorie · riferimento fisso', 's'),
                                    ('F1_continuous_fixed', 'Continuo · riferimento fisso', 'o'),
                                    ('F1_continuous_rolling5y', 'Continuo · riferimento mobile', '^')]:
            data = history[(history.strategy == name) & (history.method == kind) & (history.window == 'joint')]
            ax.plot(data.year, data.rate*100, color=COLORS[kind], marker=marker, label=label)
        ax.set_title(name)
        ax.set_xticks([2019, 2020, 2021, 2022, 2023, 2024, 2025])
        ax.set_xlim(2018.8, 2025.2)
        ax.set_ylim(0, 36)
        ax.set_xlabel('Anno valutato')
    axes[0].set_ylabel('Sedute con segnale congiunto (%)')
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='outside upper center', ncol=1, frameon=False)
    fig.supxlabel('Cronologia reale: legge invariata o cambiata sconosciuta. Questi tassi non sono falsi allarmi misurati.\n'
                  'Fisso: stime 2000–2004, normalizzazione 2005–2009, calibrazione 2010–2018. Mobile: ultimi 5 anni precedenti.', fontsize=9)
    save(fig, '03_historical_reference_sensitivity')


if __name__ == '__main__':
    main()
