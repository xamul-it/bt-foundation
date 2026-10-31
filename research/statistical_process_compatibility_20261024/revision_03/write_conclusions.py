from pathlib import Path
import json
import pandas as pd
from assess import assessment,LABELS,STRONG
ROOT=Path(__file__).resolve().parent


def pct(x):
    return f'{100*x:.2f}%'.replace('.',',')


def table(headers,rows):
    return '\n'.join(['| '+' | '.join(headers)+' |','| '+' | '.join(['---']*len(headers))+' |']+['| '+' | '.join(map(str,r))+' |' for r in rows])


def write():
    result=assessment()
    points=pd.read_csv(ROOT/'results/pointwise_summary.csv')
    monitoring=pd.read_csv(ROOT/'results/monitoring_summary.csv')
    confirm=points[points.phase.eq('confirmation')&points.window.eq('joint')]
    nulls=confirm[confirm['null'].eq(confirm.scenario)&confirm.method.eq('C5_heldout_score')]
    null_table=table(['Dado invariato','Falsi allarmi su 100 gruppi','IC 95% fra storici'],[
        [LABELS[row.null],pct(row.rate),pct(row.ci95_low)+'–'+pct(row.ci95_high)] for row in nulls.itertuples()])
    names={('iid','sticky_06'):'Da indipendente a magnetico',('sticky_06','iid'):'Scompare il magnetismo medio',
           ('cycle_forward','cycle_reverse'):'Si inverte il ciclo',('lag2','iid'):'Scompare la memoria a 2 passi',
           ('sticky_09','iid'):'Scompare il magnetismo alto',('lag5','iid'):'Scompare la memoria a 5 passi',
           ('lag7','iid'):'Scompare la memoria a 7 passi',('lag8','iid'):'Scompare la memoria a 8 passi (caso nuovo)',
           ('lag10','iid'):'Scompare la memoria a 10 passi'}
    rows=[]
    for null,scenario in [*STRONG,('lag5','iid'),('lag7','iid'),('lag8','iid'),('lag10','iid')]:
        q=confirm[confirm['null'].eq(null)&confirm.scenario.eq(scenario)]
        previous=q[q.method.eq('C3_memory_score')].iloc[0]
        current=q[q.method.eq('C5_heldout_score')].iloc[0]
        rows.append([names[(null,scenario)],pct(previous.rate),pct(current.rate),pct(current.ci95_low)+'–'+pct(current.ci95_high)])
    recognition_table=table(['Cambiamento','Prima della separazione dei dati','Candidato finale','IC 95% finale'],rows)
    selected=points[points.phase.eq('confirmation')&points.method.eq('C5_heldout_score')&points['null'].eq('sticky_06')]
    window_table=table(['Lanci nel gruppo','Falsi allarmi, dado invariato','Magnetismo rimosso: cambi riconosciuti'],[
        [window,pct(selected[selected.scenario.eq('sticky_06')&selected.window.eq(str(window))].iloc[0].rate),
         pct(selected[selected.scenario.eq('iid')&selected.window.eq(str(window))].iloc[0].rate)] for window in [3,5,10,15,20]])
    m=monitoring[monitoring.phase.eq('confirmation')]
    risk_table=table(['Periodo prefissato','Soglia puntuale ripetuta ogni volta','Soglia per il periodo intero'],[
        [str(horizon)+' controlli',pct(m[m['null'].eq('sticky_06')&m.scenario.eq('sticky_06')&m.horizon.eq(horizon)&m.method.eq('C5_daily_pointwise')].iloc[0].rate),
         pct(m[m['null'].eq('sticky_06')&m.scenario.eq('sticky_06')&m.horizon.eq(horizon)&m.method.eq('C6_period_maximum')].iloc[0].rate)] for horizon in [20,100]])
    cost_table=table(['Cambiamento','Periodo','Soglia puntuale ripetuta: riconosciuti','Soglia del periodo: riconosciuti'],[
        [names[(null,'iid')],horizon,pct(m[m['null'].eq(null)&m.scenario.eq('iid')&m.horizon.eq(horizon)&m.method.eq('C5_daily_pointwise')].iloc[0].rate),
         pct(m[m['null'].eq(null)&m.scenario.eq('iid')&m.horizon.eq(horizon)&m.method.eq('C6_period_maximum')].iloc[0].rate)]
        for null in ['sticky_06','lag10'] for horizon in [20,100]])
    capacity=[]
    for size in [2480,4960,9200]:
        old=(size-1200)//80
        point=(size-1100)//20
        banks=[(size-1100-(h+19))//(h+40)+1 for h in [20,100]]
        capacity.append([size,old,point,*banks])
    capacity_table=table(['Storia totale','Blocchi finali iniziali C0','Blocchi puntuali nuovi','Periodi da 20','Periodi da 100'],capacity)
    small=points[points.phase.eq('development')&points.history_size.eq(4960)&points['null'].eq('sticky_06')&points.method.eq('C5_heldout_score')&points.window.eq('joint')]
    mild_table=table(['Modifica della persistenza','Gruppi segnalati'],[
        [label,pct(small[small.scenario.eq(scenario)].iloc[0].rate)] for scenario,label in [
        ('sticky_05','Leggermente minore: rho 0,5'),('sticky_06','Invariata: rho 0,6'),
        ('sticky_07','Leggermente maggiore: rho 0,7'),('sticky_09','Molto maggiore: rho 0,9')]])
    manifest=json.loads((ROOT/'results/manifest.json').read_text())
    text=f'''# Candidato rivisto: cosa misura, cosa riconosce e che cosa proporre

Studio statistico del 24 ottobre 2026. **Solo simulazioni dichiarate del dado:
nessuna modifica all'applicazione e nessun risultato di strategie reali.**

## La decisione proposta

La versione finale supera i criteri fissati prima dell'ultimo esperimento.
Propongo di portare avanti **C5 come candidato per la compatibilità di un gruppo
ordinato di risultati**, mantenendo separata **C6 come possibile regola di allarme
su un periodo deciso prima**. La successiva decisione sull'applicazione deve
considerare definizione della serie reale, baseline precedente e costo dei falsi
allarmi; il risultato del dado non autorizza da solo un aggiornamento del cruscotto.

La versione finale riconosce i cinque cambiamenti forti prefissati nel 92–100%
dei gruppi; i singoli tassi sono riportati in tabella. Ha recuperato anche
le dipendenze più lunghe: circa 99% a due passi e 87% a dieci passi. Gli errori
con dado invariato restano mediamente fra {pct(result['pointwise_null_range'][0])} e {pct(result['pointwise_null_range'][1])}
nei nove tipi di riferimento della conferma. Queste medie **non garantiscono**
lo stesso errore per una qualunque singola storia reale.

## 1. Cosa è cambiato e perché

Il candidato iniziale prima trasformava ogni indizio in una classifica e poi
classificava l'insieme. Con pochi esempi storici, due sequenze molto diverse
potevano ricevere lo stesso valore estremo. Abbiamo eliminato questo passaggio:
ora si conserva quanto è grande ogni scarto, e si confronta con la storia
soltanto il risultato finale.

La prima correzione recuperava certi cambiamenti ma ne perdeva altri. È stato
quindi aggiunto un confronto esplicito fra risultati separati da uno, due, fino
a dieci passi: lo storico insegna quanto sia normale ripetere un valore a quella
distanza. Non si assume che il magnetismo dipenda soltanto dal lancio precedente.

Infine è stata corretta la costruzione del riferimento. Se stimo le frequenze
su certi dati e uso gli stessi dati per decidere quanto sono normali gli scarti,
quegli scarti possono apparire artificialmente piccoli. Le altre componenti
finiscono così per essere sommerse quando arrivano dati nuovi. **Ora i dati per
apprendere le frequenze sono distinti da quelli per stabilire la scala degli
scarti**: primi 500 risultati per il primo compito, successivi 500 per il secondo.
Poi si lascia un intervallo e si usano soltanto i dati seguenti per la calibrazione
finale. In totale il segmento iniziale resta di 1.000 dati, come nei confronti.

Tutti i candidati sono stati confrontati sugli stessi nuovi storici e sugli stessi
gruppi recenti. I risultati usati per correggere una versione non sono stati
spacciati per conferma della successiva: ogni revisione ha nuovi seed e nuovi
campioni, e i fallimenti precedenti restano archiviati in revision_01 e revision_02.

## 2. Come leggere le percentuali qui sotto

Ci sono due domande diverse:

- **Dado invariato:** su cento gruppi, quanti vengono segnalati erroneamente come
  sospetti? Questi sono falsi allarmi; l'obiettivo nominale è circa cinque.
- **Dado cambiato:** su cento gruppi, in quanti viene riconosciuto il cambiamento?
  Qui un valore alto indica maggiore capacità di rilevazione.

Non sono percentuali di probabilità che il dado sia ancora lo stesso.
La misura finale di compatibilità è un rango di anomalia rispetto alla storia.
Un valore alto significa che le caratteristiche misurate non forniscono evidenza
contro quel riferimento. Un valore basso non identifica da solo il motivo.

La conferma principale usa **20 storici indipendenti di 4.960 risultati**, ciascuno
con 2.000 gruppi recenti per caso. Ci sono 40.000 gruppi per riga, ma tutti quelli
che condividono lo stesso storico condividono anche l'incertezza di quel riferimento.
Gli intervalli principali sono quindi calcolati fra i 20 storici, non fingendo
40.000 repliche indipendenti della baseline. Sono IC t approssimati, non garanzie
finite. Vicino a zero e uno possono collassare: un 100% osservato non prova
probabilità esattamente uguale a uno.

## 3. Con dado invariato: quanti falsi allarmi?

Ogni riga valuta una sola volta il confronto comune delle finestre 3/5/10/15/20:

{null_table}

![Falsi allarmi](figures/01_false_alarms.png)

Il caso con memoria a otto passi non era stato usato nelle revisioni precedenti.
È un controllo aggiuntivo all'interno della stessa famiglia sintetica, non una
prova di generalizzazione a ogni legge possibile. Gli estremi fra singoli storici
sono nei CSV: il livello medio vicino al 5% non cancella quell'incertezza.

## 4. Quando il dado cambia: quanti cambiamenti vengono riconosciuti?

Qui ogni gruppo contiene 20 risultati della nuova legge e viene valutato una volta.
La tabella confronta C3 e C5 sullo stesso campione di conferma finale; i numeri
possono differire da quelli della conferma precedente, che usava altri storici.

{recognition_table}

![Riconoscimento dei cambiamenti](figures/02_change_recognition.png)

È utile conservare anche i casi in cui il miglioramento non è uniforme: la nuova
versione può rinunciare a un po' di sensibilità in un caso per non essere cieca
alla memoria più lunga. La scelta non deriva dal solo numero di segnalazioni.

### Gruppi molto corti

Con riferimento magnetico medio, confronto fra dado invariato e magnetismo rimosso:

{window_table}

A tre o cinque osservazioni la capacità di riconoscere il cambiamento rimane
inferiore. Non prometterei una diagnosi affidabile di qualsiasi modifica dopo
tre giorni soltanto. Il confronto comune delle cinque finestre viene calibrato
insieme; scegliere dopo il test la finestra più allarmante cambierebbe il problema.

## 5. Per i controlli ripetuti serve un'altra regola

Consideriamo cento periodi distinti, **ciascuno lungo cento controlli**, durante
i quali il dado magnetico medio rimane sempre invariato. Usare ogni giorno la
soglia puntuale produce almeno un falso allarme in circa 73 periodi su cento.
Con C6 la soglia viene invece scelta guardando il peggiore scarto dell'intero
periodo storico: il risultato scende a circa quattro periodi su cento.

{risk_table}

Le percentuali di questa tabella contano **periodi con almeno un allarme**, non
singoli gruppi. La conferma usa 20 storici e 100 traiettorie per storico e caso.
L'ipotesi non cambia durante le traiettorie nulle; nelle alternative il cambiamento
avviene al primo nuovo risultato, mantenendo il contesto del dado precedente.

![Falsi allarmi sul periodo intero](figures/03_period_false_alarms.png)

Il periodo deve essere fissato prima. Ripartire automaticamente ogni mese con un
nuovo 5% farebbe accumulare di nuovo il rischio. Non abbiamo costruito una garanzia
valida per ogni data futura né un meccanismo automatico di intervento.

### Il costo della prudenza

Ridurre i falsi allarmi ha un costo: alcune modifiche vengono riconosciute meno
spesso o più tardi. Sugli stessi periodi nei quali il dado cambia:

{cost_table}

La soglia del periodo di 100 controlli non è semplicemente la soglia da 20 usata
più a lungo: è più severa, per proteggere un periodo più ampio. Nel caso della
memoria a dieci passi, il riconoscimento scende all'incirca dall'82% al 46% passando
fra queste due regole. **Perciò non propongo automaticamente l'orizzonte più lungo.**
La scelta deve dichiarare quanti falsi allarmi si accettano e quali cambiamenti si
vuole riconoscere in tempi utili. I tempi medi/mediani solo tra i casi rilevati
non vanno interpretati come ritardo garantito anche per i casi non rilevati.

## 6. Lo storico corto ora viene usato meglio, ma resta finito

Con il medesimo numero totale di osservazioni:

{capacity_table}

Il candidato iniziale, con 2.480 osservazioni, aveva 16 esempi per l'ultima
calibrazione: la percentuale minima era 1/17 = 5,88%, quindi non poteva mai
superare la soglia del 5%. Il nuovo candidato puntuale ha 69 esempi e non perde
informazione attraverso il doppio rango. Non abbiamo inventato nuova storia
moltiplicando i ricampionamenti.

I blocchi sono non sovrapposti ma successivi: **non vengono dichiarati indipendenti**.
La verifica su molti storici stima empiricamente l'effetto della dipendenza, non
lo elimina. Per un periodo di 100 controlli, con 2.480 dati restano soltanto dieci
periodi di calibrazione: C6 è dichiarato *non stimabile al livello scelto*.
Il silenzio in quel caso non è prova che il processo sia stabile.

Anche con 4.960 dati ci sono solo 27 periodi da 100 per calibrare C6: la coda è
povera e il rischio per un singolo storico può essere incerto. Le medie favorevoli
nelle simulazioni non rendono esatta la probabilità di falso allarme su una
baseline specifica. Questo pesa sulla scelta di un eventuale allarme operativo.

## 7. Cosa rimane difficile

Nei test di sviluppo, cambiando poco la persistenza del dado medio:

{mild_table}

![Cambiamenti piccoli](figures/04_small_changes.png)

La persistenza invariata genera falsi allarmi; negli altri casi le segnalazioni
misurano rilevazione. La capacità per cambiamenti lievi resta bassa e non va
confusa con una prova di equivalenza. Le cinque famiglie forti e i ritardi
aggiuntivi sono controlli mirati, non un inventario di tutte le alternative.

Lo studio resta sintetico e categorico. Il fatto che una faccia cada con una
certa frequenza non esaurisce una serie di rendimenti: contano ampiezza delle
perdite, code, volatilità, costi ed esposizione. Una futura eventuale codifica in
categorie deve conservare componenti numeriche per non nascondere un crollo
che ricada nella stessa categoria. Non è stata scelta o validata una tale
trasformazione sui dati reali.

## 8. Criteri dichiarati e proposta concreta per la decisione successiva

La versione finale ha passato i criteri del protocollo R03:

- limite superiore degli IC dei falsi allarmi sotto 7,5% in tutti i null della
  conferma, sia per un controllo comune alle finestre, sia per periodi di 20/100;
- almeno 80% di riconoscimento in almeno quattro dei cinque cambiamenti forti:
  risultato osservato, cinque su cinque;
- nessuna cecità sistematica ai cambiamenti forti;
- almeno 50% di riconoscimento per rimozione di memoria a cinque, sette e dieci
  passi: tutti e tre i casi superano il criterio.

L'obiettivo nominale resta 5%; il 7,5% è una tolleranza fissata per decidere se
proseguire la ricerca, non una nuova definizione del test. I valori e tutti i
controlli sono in results/assessment.json. Le garanzie sotto dipendenza richiedono
assunzioni esplicite; nessuno dei paper citati certifica automaticamente C5/C6.

**Proposta:** scegliere C5 come candidato della futura verifica su una serie reale
congelata; trattare l'eventuale allarme C6 come scelta separata. La prima eventuale
applicazione dovrebbe essere diagnostica, senza automatismi di stop o cambio
strategia. Per decidere la sua introduzione nel cruscotto servono:

1. la variabile finanziaria esatta: rendimenti giornalieri netti comparabili,
   calendario, posizioni overnight e date di esiti completi;
2. una baseline antecedente al periodo che si valuta, escludendo sovrapposizioni
   che rendano il confronto soltanto retrospettivo;
3. controllo su dati reali della calibrazione, delle caratteristiche numeriche
   e della sensibilità a cambiamenti rilevanti;
4. se si vuole anche un allarme, scelta esplicita di orizzonte, tolleranza agli
   errori e costo dei cambiamenti persi.

Il candidato statistico è ora definito e documentato. La decisione applicativa
concordata richiede la verifica del suo significato sui dati finanziari che dovrà
misurare.

## Evidenza e riproducibilità

- [Notebook eseguito](studio.ipynb), [versione leggibile](studio.html).
- [Protocollo](protocol.md), [fonti primarie](sources.md).
- results/: tutte le righe, comprese revisioni peggiori, casi lievi e non stimabili.
- inputs/: storici, gruppi e traiettorie sintetici conservati per verifica.
- results/validation.json: verifiche indipendenti su formule tramite conteggi
  espliciti, separazione dei dati, rangi e periodi; controlli numerici generali.
- results/manifest.json: seed, dipendenze, hash e risorse. Questo motore ha
  impiegato circa {manifest['elapsed_seconds']:.0f} secondi su un core, con picco circa
  {manifest['peak_rss_mib']:.0f} MiB. Sono risorse di R03, non di un backtest economico.

Le precedenti cartelle di studio restano disponibili per ricostruire i tre
passaggi e i criteri falliti. Il notebook finale analizza risultati già eseguiti
e conservati; contiene anche il comando per rigenerarli integralmente.
'''
    (ROOT/'conclusioni.md').write_text(text)


if __name__=='__main__':
    write()
