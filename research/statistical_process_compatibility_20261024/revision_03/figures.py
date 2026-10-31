from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter
import numpy as np
import pandas as pd
from assess import LABELS, STRONG

ROOT=Path(__file__).resolve().parent
BLUE='#2563EB'; ORANGE='#B45309'; GRAY='#6B7280'


def setup():
    plt.rcParams.update({'font.size':11,'axes.titlesize':13,'axes.labelsize':11,
                         'figure.facecolor':'white','axes.facecolor':'white',
                         'axes.spines.top':False,'axes.spines.right':False,
                         'axes.edgecolor':'#374151','text.color':'#111827','axes.labelcolor':'#111827'})


def save(fig,name):
    for extension in ['png','svg']:
        fig.savefig(ROOT/'figures'/f'{name}.{extension}',dpi=150,bbox_inches='tight')
    return fig


def false_alarms():
    setup()
    data=pd.read_csv(ROOT/'results/pointwise_summary.csv')
    data=data[data.phase.eq('confirmation')&data['null'].eq(data.scenario)&data.window.eq('joint')]
    order=['iid','sticky_06','sticky_09','cycle_forward','lag2','lag5','lag7','lag8','lag10']
    fig,ax=plt.subplots(figsize=(10,6.4))
    for offset,method,label,color,marker in [(-.1,'C0_double_rank','Candidato iniziale (C0)',GRAY,'s'),
                                             (.1,'C5_heldout_score','Candidato finale (C5)',BLUE,'o')]:
        rows=data[data.method.eq(method)].set_index('null').loc[order]
        ax.errorbar(rows.rate,np.arange(len(order))+offset,
                    xerr=[rows.rate-rows.ci95_low,rows.ci95_high-rows.rate],
                    fmt=marker,capsize=3,color=color,label=label)
    ax.axvline(.05,color='#374151',linestyle='--',label='Obiettivo nominale: 5%')
    ax.set(yticks=np.arange(len(order)),yticklabels=[LABELS[x] for x in order],
           xlabel='Gruppi segnalati per errore con dado invariato',xlim=(0,.105),
           title='Un controllo delle cinque finestre, in un singolo istante')
    ax.invert_yaxis();ax.xaxis.set_major_formatter(PercentFormatter(1))
    ax.grid(axis='x',alpha=.15)
    handles,labels=ax.get_legend_handles_labels()
    fig.legend(handles,labels,frameon=False,loc='upper center',bbox_to_anchor=(.55,.91),fontsize=9)
    fig.text(.04,.02,'Conferma indipendente: 20 storici di 4.960 osservazioni, 2.000 gruppi per storico.\nIC t 95% fra storici: approssimati; il basso numero di segnalazioni di C0 non ne misura la potenza.',fontsize=10)
    fig.tight_layout(rect=(0,.08,1,.80))
    return save(fig,'01_false_alarms')


def recognition():
    setup()
    data=pd.read_csv(ROOT/'results/pointwise_summary.csv')
    data=data[data.phase.eq('confirmation')&data.window.eq('joint')]
    names=['Scompare il magnetismo\n(persistenza media)','Scompare la memoria\na 2 passi','Scompare la memoria\na 5 passi','Scompare la memoria\na 7 passi','Scompare la memoria\na 8 passi (nuovo caso)','Scompare la memoria\na 10 passi']
    pairs=[('sticky_06','iid'),('lag2','iid'),('lag5','iid'),('lag7','iid'),('lag8','iid'),('lag10','iid')]
    fig,ax=plt.subplots(figsize=(10.5,6.2))
    for offset,method,label,color,marker in [(-.13,'C3_memory_score','Prima della separazione dei dati (C3)',ORANGE,'s'),
                                             (.13,'C5_heldout_score','Con riferimento separato (C5)',BLUE,'o')]:
        selected=[data[data['null'].eq(null)&data.scenario.eq(scenario)&data.method.eq(method)].iloc[0] for null,scenario in pairs]
        means=np.array([r.rate for r in selected])
        lo=np.array([r.ci95_low for r in selected]);hi=np.array([r.ci95_high for r in selected])
        ax.errorbar(means,np.arange(len(pairs))+offset,xerr=[means-lo,hi-means],fmt=marker,
                    color=color,capsize=3,label=label)
    ax.axvline(.8,color='#374151',linestyle='--',label='Riferimento visivo: 80%')
    ax.set(yticks=np.arange(len(pairs)),yticklabels=names,xlim=(-.02,1.03),
           xlabel='Gruppi nei quali viene riconosciuto il cambiamento',
           title='La separazione dei dati migliora il riconoscimento della memoria')
    ax.invert_yaxis();ax.xaxis.set_major_formatter(PercentFormatter(1));ax.grid(axis='x',alpha=.15)
    handles,labels=ax.get_legend_handles_labels()
    fig.legend(handles,labels,frameon=False,loc='upper center',bbox_to_anchor=(.62,.92),fontsize=9)
    fig.text(.04,.02,'Conferma su nuove simulazioni; 20 storici e 40.000 gruppi per caso e metodo, confronti appaiati.\nIC t 95% fra storici; valori osservati al 100% non provano potenza esattamente pari a uno.',fontsize=10)
    fig.tight_layout(rect=(0,.08,1,.80))
    return save(fig,'02_change_recognition')


def repeated():
    setup()
    data=pd.read_csv(ROOT/'results/monitoring_summary.csv')
    data=data[data.phase.eq('confirmation')&data['null'].eq(data.scenario)]
    order=['iid','sticky_06','sticky_09','cycle_forward','lag2','lag5','lag7','lag8','lag10']
    short=['iid','sticky .6','sticky .9','ciclo','lag 2','lag 5','lag 7','lag 8','lag 10']
    fig,axes=plt.subplots(1,2,figsize=(12,6.2),sharey=True)
    for ax,horizon in zip(axes,[20,100]):
        for method,label,color,marker in [('C5_daily_pointwise','Soglia puntuale ogni giorno',ORANGE,'s'),
                                          ('C6_period_maximum','Soglia per tutto il periodo',BLUE,'o')]:
            rows=data[data.method.eq(method)&data.horizon.eq(horizon)].set_index('null').loc[order]
            ax.errorbar(np.arange(len(order)),rows.rate,
                        yerr=[rows.rate-rows.ci95_low,rows.ci95_high-rows.rate],
                        fmt=marker,color=color,capsize=3,label=label)
        ax.axhline(.05,color='#374151',linestyle='--',linewidth=1)
        ax.set(xticks=np.arange(len(order)),xticklabels=short,ylim=(0,1),title=f'Periodo fissato: {horizon} controlli')
        ax.tick_params(axis='x',rotation=45)
        ax.yaxis.set_major_formatter(PercentFormatter(1));ax.grid(axis='y',alpha=.15)
    axes[0].set_ylabel('Periodi con almeno un falso allarme')
    handles,labels=axes[0].get_legend_handles_labels()
    fig.legend(handles,labels,loc='upper center',bbox_to_anchor=(.5,.92),frameon=False,ncol=2)
    fig.suptitle('Il rischio viene misurato sul periodo intero, con dado invariato',fontsize=14,y=.99)
    fig.text(.04,.02,'Conferma: 20 storici, 100 traiettorie per storico e caso. IC t 95% fra storici, approssimati.\nIl risultato riguarda il periodo prefissato, non mesi rinnovati o un orizzonte illimitato.',fontsize=10)
    fig.tight_layout(rect=(0,.10,1,.85))
    return save(fig,'03_period_false_alarms')


def subtle_changes():
    setup()
    data=pd.read_csv(ROOT/'results/pointwise_summary.csv')
    data=data[data.phase.eq('development')&data['null'].eq('sticky_06')&data.history_size.eq(4960)&data.method.eq('C5_heldout_score')&data.window.eq('joint')]
    order=['sticky_05','sticky_06','sticky_07','sticky_09','iid']
    names=['Persiste meno\nrho .5','Invariato\nrho .6','Persiste più\nrho .7','Persiste molto più\nrho .9','Magnetismo rimosso\niid']
    rows=data.set_index('scenario').loc[order]
    fig,ax=plt.subplots(figsize=(10,5.4))
    ax.bar(np.arange(5),rows.rate,color=BLUE,width=.55)
    ax.errorbar(np.arange(5),rows.rate,yerr=[rows.rate-rows.ci95_low,rows.ci95_high-rows.rate],fmt='none',color='#111827',capsize=4)
    ax.axhline(.05,color='#374151',linestyle='--',label='Obiettivo nominale di falso allarme')
    ax.set(xticks=np.arange(5),xticklabels=names,ylim=(0,1.05),
           ylabel='Gruppi segnalati',title='Cambiamenti piccoli restano difficili da riconoscere in 20 lanci')
    ax.yaxis.set_major_formatter(PercentFormatter(1));ax.grid(axis='y',alpha=.15)
    ax.legend(frameon=False,loc='upper left')
    fig.text(.04,.02,'Sviluppo, non conferma: 20 storici di 4.960 osservazioni, 1.000 gruppi per storico e scenario.\nPer rho .6 le segnalazioni sono falsi allarmi; per gli altri casi misurano riconoscimento.',fontsize=10)
    fig.tight_layout(rect=(0,.09,1,1))
    return save(fig,'04_small_changes')
