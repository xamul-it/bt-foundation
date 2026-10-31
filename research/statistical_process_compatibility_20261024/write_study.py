from pathlib import Path
import json
import pandas as pd

ROOT = Path(__file__).resolve().parent


def percent(value):
    return f'{100 * value:.2f}%'.replace('.', ',')


def make_table(headers, rows):
    return '\n'.join(['| ' + ' | '.join(headers) + ' |', '| ' + ' | '.join(['---'] * len(headers)) + ' |'] + ['| ' + ' | '.join(map(str, row)) + ' |' for row in rows])


def write():
    rates = pd.read_csv(ROOT / 'results/rejection_rates.csv')
    exact = json.loads((ROOT / 'results/exact_probabilities.json').read_text())
    monitor = pd.read_csv(ROOT / 'results/monitoring.csv')
    variability = pd.read_csv(ROOT / 'results/baseline_variability.csv')
    labels = {'iid': 'Indipendente uniforme', 'sticky_06': 'Magnetico sticky', 'cycle_forward': 'Magnetico direzionale', 'lag2': 'Memoria a due passi'}
    def cell(null, scenario, size, method):
        row = rates[(rates['null'] == null) & (rates.scenario == scenario) & (rates.window == size) & (rates.method == method)].iloc[0]
        return percent(row.rate)
    null_table = make_table(['Processo invariato, 20 lanci', 'Test media iid', 'Ensemble temporale', 'IC 95% ensemble'], [
        [labels[null], cell(null, null, 20, 'mean_iid'), cell(null, null, 20, 'ensemble5_blocks'),
         f"{percent(row.ci95_low)}–{percent(row.ci95_high)}"]
        for null in labels for row in [rates[(rates['null'] == null) & rates.is_null & (rates.window == 20) & (rates.method == 'ensemble5_blocks')].iloc[0]]])
    power_table = make_table(['Cambiamento dalla baseline sticky', '3 lanci', '5', '10', '15', '20'], [
        [label] + [cell('sticky_06', scenario, size, 'ensemble5_blocks') for size in [3,5,10,15,20]]
        for scenario, label in [('sticky_06', 'Nessun cambiamento: falsi allarmi'), ('iid', 'Magnetismo rimosso'), ('sticky_09', 'Magnetismo aumentato')]])
    monitor_table = make_table(['Processo invariato', '5 finestre senza correzione', 'Correzione fra finestre allo stesso istante'], [
        [labels[null]] + [percent(monitor[(monitor['null'] == null) & (monitor.method == method)].iloc[0].any_alarm_rate) for method in ['five_windows_unadjusted', 'five_windows_jointly_calibrated']]
        for null in labels])
    example_table = make_table(['Legge del dado', 'P(10 sei specifici)', 'P(10 facce tutte uguali)'], [
        [label, f"{exact[str(rho)]['probability_ten_sixes']:.10g}", f"{exact[str(rho)]['probability_ten_identical']:.10g}"]
        for rho, label in [(0., 'Indipendente'), (.6, 'Sticky: ripete con probabilità 2/3'), (.9, 'Sticky: ripete con probabilità 11/12')]])
    finite = pd.read_csv(ROOT / 'results/finite_history.csv')
    finite_rows = []
    for (blocks, observations, scenario), group in finite[finite.method == 'global_ensemble5'].groupby(['blocks','observations','scenario']):
        finite_rows.append([int(observations), int(blocks), scenario, percent(group.rate.min()), percent(group.rate.median()), percent(group.rate.max())])
    finite_table = make_table(['Storia totale', 'Blocchi per calibrazione', 'Scenario', 'Minimo', 'Mediana', 'Massimo'], finite_rows)
    text = f'''# Compatibilità di una sequenza con un processo storico dipendente

Studio statistico — 24 ottobre 2026. **Dati sintetici dichiarati; nessuna strategia
reale valutata e nessuna modifica al calcolo applicativo.**

## Decisione proposta

Per il problema concordato serve confrontare **blocchi consecutivi e caratteristiche
della loro dinamica**, imparandone la distribuzione dallo storico. La sola media
con errore standard da osservazioni indipendenti non risponde alla domanda.

La famiglia da portare alla successiva validazione è una **misura di anomalia del
blocco, con score complementari e calibrazione cronologica su blocchi storici**.
Un modello predittivo temporale può essere una componente, non una legge assunta
vera. Lo studio sostiene questa famiglia; non certifica lo score sperimentale a
cinque componenti come soluzione pronta per i rendimenti reali. **Nell'estensione
con storici corti proprio quell'aggregato può perdere completamente potenza: non
propongo di trasferirlo nell'applicazione nella forma sperimentata.**

Ci sono tre problemi distinti da risolvere: quale aspetto della sequenza rilevare,
come calibrare il confronto sotto dipendenza, come gestire molte finestre e molti
controlli nel tempo. Nessun singolo p-value dimostra che il processo sia rimasto
uguale. Un test può rilevare anomalie senza identificare la causa o la perdita
dell'edge di una strategia.

## 1. La domanda statistica esatta

Indichiamo con X_t il risultato ordinato nel tempo e con F_n la legge congiunta di
n osservazioni consecutive sotto il riferimento storico. L'ipotesi operativa è
che il blocco recente venga dalla stessa F_n. Uguali medie, varianze e frequenze
marginali non implicano uguali F_n. Per sostenere l'uguaglianza dell'intero processo
servirebbe uguaglianza a tutte le lunghezze: con dati finiti possiamo verificare
solo differenze osservabili alle scale e con gli score scelti.

Dobbiamo anche dichiarare il condizionamento: confronto con un blocco storico
qualsiasi, oppure con un blocco che inizi nello stesso stato osservabile? Le due
domande hanno risposte diverse. Uno stato latente persistente può generare un
periodo insolito pur con legge invariata. Se la legge può essere arbitraria,
non stazionaria o dotata di memoria illimitata, una storia finita non identifica
il magnetismo: non esiste qui una garanzia universale senza assunzioni.
Un esempio limite: due processi possono avere la stessa legge di ogni blocco
lungo 20 e divergere soltanto a lunghezze maggiori. Un processo costruito come
21 catene sticky indipendenti interlacciate ha blocchi di 20 osservazioni iid,
ma dipendenza al ritardo 21. Nessun test sul solo blocco isolato di 20 può
riconoscere quella differenza rispetto a iid. Un contesto precedente più lungo
cambia la domanda e l'informazione disponibile.

### Tre probabilità da non confondere

- **Probabilità della sequenza esatta sotto un modello**: P(blocco esatto | H0).
  Per valori continui un punto ha probabilità zero: occorre una densità o un evento.
- **P-value di anomalia**: P(score almeno altrettanto estremo | H0), secondo una
  definizione di anomalia scelta prima del test. Non è una probabilità di H0.
- **Probabilità che il dado sia ancora quello storico**: P(H0 | dati). Richiede un
  modello bayesiano anche per le alternative e probabilità iniziali. Non si ottiene
  facendo 1−p o moltiplicando le frequenze delle singole facce.

Con un dado iid uniforme, tutte le sequenze specifiche di n facce hanno probabilità
6^(-n). Quindi la sola probabilità esatta non distingue dieci sei da un qualsiasi
altro percorso di dieci lanci. Serve una proprietà: per esempio una serie di facce
identiche, o una relazione predittiva. L'esempio di cinquanta uno può già spostare
la distribuzione marginale: la prova decisiva dell'ordine usa **gli stessi valori
in ordine diverso**.

### Se fosse indispensabile una probabilità di permanenza

Una formulazione bayesiana è possibile, ma va definito quali sostituzioni sono
ammesse e con quali probabilità iniziali. Esempio puramente analitico, con prior
uguali: dieci sei sotto H0 sticky rho=0,6 hanno verosimiglianza 0,004335.
Con alternativa iid, la probabilità a posteriori di H0 è circa 99,9996%; con
alternativa sticky rho=0,9 scende invece a circa 5,39%. Sono gli stessi dati e
la stessa H0: cambia l'alternativa. Questi numeri illustrano la dipendenza dal
modello, non stimano quale dado reale sia presente. Una miscela di modelli
predittivi e di possibili cambiamenti è una seconda famiglia da studiare se
si vuole proprio una probabilità posteriore; richiede controlli di specificazione
e sensibilità alle prior. Lo score di anomalia non va rinominato come tale.

## 2. Il dado magnetico, definito matematicamente per verificarci

Un generatore di controllo semplice è P_ij = rho·I(i=j) + (1−rho)/6. Le righe e le
colonne sommano a uno: iniziando uniformemente, ogni faccia resta marginalmente a
1/6. Con rho=0,6 la probabilità di ripetere è 2/3; con rho=0,9 è 11/12. Il generatore
è noto solo nell'esperimento, per sapere quando stiamo sbagliando; gli score vedono
soltanto lo storico simulato e non ricevono rho.

{example_table}

Per un blocco prefissato, P(10 sei)=(1/6)·q^9 e P(10 facce uguali)=q^9, dove q è
la probabilità di ripetizione. Il fattore sei dipende da quale evento è stato
scelto. Cercare la serie dopo aver visto molti dati richiede un'ulteriore correzione.

Il confine conta: sotto rho=0,6, dieci **nuovi** sei hanno probabilità
{percent(exact['0.6']['probability_ten_new_sixes_given_previous_six'])} se il risultato appena precedente era sei,
contro {percent(exact['0.6']['probability_ten_new_sixes_given_previous_one'])} se era uno.
Non sono le probabilità incondizionate della tabella.

Sono stati aggiunti un ciclo direzionale e un processo con memoria al secondo
passo. Quest'ultimo mantiene le coppie adiacenti marginalmente indipendenti ma
ripete X_(t−2) con probabilità 5/6. È una prova contro l'assunzione che un solo
ritardo descriva tutta la dipendenza.

## 3. Che cosa è stato confrontato

Ogni riferimento: 20.000 dati di training; due segmenti successivi separati per
calibrare gli score e l'aggregazione, 1.000 blocchi ciascuno, distanziati di 40
osservazioni oltre la lunghezza massima. Valutazione: 10.000 episodi indipendenti
per ogni coppia riferimento/scenario, finestre 3/5/10/15/20, soglia nominale 5%.
Totale: quattro riferimenti, sette scenari per riferimento. Gli stessi episodi
sono riutilizzati fra metodi e finestre: quei confronti non sono indipendenti.
Lo storico non contiene i dati di test. Protocollo e addendum precedono l'esecuzione.

| Procedura | Informazione usata | Limite principale |
| --- | --- | --- |
| Z della media, iid | Media, dispersione, numerosità | Errori standard sbagliati sotto dipendenza; ignora l'ordine |
| Istogramma, calibrato su blocchi | Composizione dei risultati e sua variabilità storica | Stesso multinsieme riordinato dà lo stesso score |
| Sorpresa Markov ordine 1, due code | Probabilità stimata del successore dato il predecessore | Memoria più lunga e score inadatto possono renderlo debole |
| Quattro caratteristiche | Istogramma, ripetizioni a lag 1/2, serie identica massima | Non distingue la direzione di un ciclo |
| Ensemble di cinque componenti | Le quattro precedenti più sorpresa Markov | Più ampio ma ancora non universale; richiede calibrazione |

Per i ranghi empirici si includono i pareggi in modo conservativo. Due segmenti
di calibrazione evitano di usare gli stessi dati per costruire i ranghi e per
stimare la coda dell'aggregato. Blocchi distanziati non diventano automaticamente
scambiabili: la validità sotto dipendenza è approssimata e va verificata.
**Non è un'implementazione di conformal esatta né eredita i teoremi di un paper.**

## 4. Falsi allarmi: la calibrazione viene prima della potenza

Con il processo invariato, su finestre di 20 osservazioni:

{null_table}

![Falsi allarmi](figures/01_false_alarms.png)

Il test della media arriva al 31,65% sul dado sticky e al 41,92% su quello con
memoria a due passi. Sul ciclo direzionale è invece molto conservativo: dipendenza
non significa necessariamente inflazione degli errori nella stessa direzione.

Anche l'ensemble non mantiene esattamente il 5%: raggiunge 5,68% sul riferimento iid
e 5,85% sul lag 2. Gli intervalli mostrati condizionano sul riferimento stimato:
non includono l'incertezza di ricostruirlo da un'altra storia. In 20 repliche di
baseline sticky ridotte, il tasso dell'ensemble da 20 lanci va dal
{percent(variability[variability.method=='ensemble5_blocks'].rate.min())} al {percent(variability[variability.method=='ensemble5_blocks'].rate.max())}.
Questa variabilità è un risultato, non un problema da nascondere scegliendo una
baseline favorevole.

Per la media sticky rho=0,6, la covarianza al ritardo k è sigma²·rho^k. La varianza
della media di 20 lanci è {exact['0.6']['variance_inflation_n20']:.3f} volte quella iid: il numero equivalente
per stimare **quella media** è circa {20/exact['0.6']['variance_inflation_n20']:.2f}, non 20.
Non esiste un unico numero effettivo valido per qualunque statistica del blocco.

## 5. Potenza: quando si accorge che il magnetismo è cambiato?

Frequenze di segnalazione dell'ensemble, con baseline sticky rho=0,6:

{power_table}

![Potenza e lunghezza](figures/02_power_by_length.png)

Rimuovere quel magnetismo è facile a 20 lanci (99,41%); aumentarlo a rho=0,9 è
rilevato nel 67,53% dei casi. Con 3–5 lanci, l'aumento di persistenza è quasi sempre
invisibile allo score adottato. Il motivo è anche discreto: una breve serie
identica è già frequente nel riferimento. Non si può promettere una diagnosi
sensibile a qualsiasi cambio in tre giorni. Queste alternative sono forti e
specifiche: non dimostrano sensibilità a cambiamenti lievi dei rendimenti.

### Caso cieco e utilità degli score complementari

Invertendo il verso del ciclo magnetico, il test della media segnala solo lo
0,40% dei blocchi di 20 lanci; le quattro caratteristiche simmetriche il 4,34%.
La sorpresa Markov e l'ensemble arrivano al 100% in questo scenario forte.
Il caso opposto è la memoria al secondo passo: un predittore di ordine 1 non basta
come descrizione della legge. Se fosse esattamente iid uniforme, il suo score
sarebbe n·log(6) per ogni percorso, anche per uno generato da una legge diversa.
Il modello stimato finitamente qui può captare parte della differenza per altre
ragioni, ma non possiede una garanzia universale di rilevarla.

Gli score simmetrici e Markov sono invarianti a una rietichettatura coerente delle
facce; lo Z della loro media numerica non lo è. Per rendimenti finanziari il valore
numerico ha invece un significato economico: questa è una differenza da preservare.

## 6. Cinque finestre e cento controlli sono due correzioni diverse

L'aggregato globale tiene conto delle cinque finestre osservate **allo stesso
istante**, calibrandone il massimo su blocchi storici con gli stessi suffix.
I suoi falsi allarmi puntuali nei quattro null vanno dal 2,99% al 4,69%.
Conservatività e discrezione hanno anche un costo di potenza.

Su 100 controlli consecutivi del processo invariato, percentuale delle traiettorie
con almeno una segnalazione:

{monitor_table}

![Monitoraggio ripetuto](figures/03_repeated_monitoring.png)

Sono 1.000 traiettorie indipendenti per riferimento; le finestre entro ogni
traiettoria sono sovrapposte e dipendenti. Gli intervalli Wilson sono in
results/monitoring.csv. Correggere le finestre allo stesso istante lascia comunque
il 72,2–76,0% di almeno un allarme in cento controlli in questi esperimenti.
Non è una contraddizione: il livello puntuale non controlla tutto il periodo.

Per un uso giornaliero bisogna scegliere prima l'obiettivo: probabilità di almeno
un falso allarme su un orizzonte, frequenza attesa o tempo medio prima di un falso
allarme. Una misura descrittiva puntuale può restare visibile ogni giorno;
una regola di allarme/intervento richiede calibrazione aggiuntiva su traiettorie
o una procedura sequenziale con le proprie assunzioni. Le confidence sequence
e gli e-process non garantiscono validità contro una dipendenza arbitraria.

## 7. Perché non basta dire «facciamo un bootstrap»

![Sensibilità alla lunghezza dei blocchi](figures/04_bootstrap_sensitivity.png)

Nello scenario sticky, il bootstrap di singole osservazioni produce circa il
29,48% di falsi allarmi per la regione centrale empirica della media di 20 lanci.
Con blocchi geometrici di lunghezza media 2 si resta al 16,38%; con 10 si scende
al 6,46%; con 50 al 5,09%. **50 non è una raccomandazione universale**: qui il
processo e il criterio sono specifici. Blocchi troppo corti spezzano la memoria;
blocchi molto lunghi riducono la diversità e il numero effettivo di repliche
storiche. In una serie non stazionaria possono anche riprodurre una miscela inadatta.

I blocchi storici contigui sono un primo riferimento semplice per finestre brevi:
conservano esattamente ciò che è avvenuto al loro interno. Il bootstrap a blocchi
può aiutare a studiare l'incertezza e orizzonti più lunghi, a patto di fare analisi
di sensibilità. Un gran numero di ricampionamenti non crea nuova storia.

### Capacità di uno storico realistico: estensione separata

Il confronto principale usa 140.400 osservazioni per riferimento: alla convenzione
illustrativa di 252 sessioni/anno sarebbero oltre 550 anni. Serve a isolare il
problema statistico del dado, non è un requisito realistico accettabile per una
strategia. Anche le repliche ridotte precedenti restano lunghe rispetto alla
storia normalmente disponibile.

L'estensione finite_history_extension.md, dichiarata prima di eseguirla ma
successiva all'esperimento principale, usa 2.480, 4.960 e 9.200 osservazioni,
training di 1.000 e rispettivamente 16, 47, 100 blocchi per calibrazione. Ripete
venti baseline per dimensione, con 2.000 episodi per scenario e baseline.
Con 16 blocchi il rango minimo è 1/17 = 5,88%: al livello 5% non si può segnalare
nulla con questa procedura non randomizzata. Questo non è evidenza di stabilità.
Con 47 blocchi la risoluzione minima è 2,08%: la coda è ancora grossolana e
la variabilità fra baseline va riportata.

{finite_table}

La tabella mostra minimo/mediana/massimo delle percentuali di segnalazione fra
20 baseline, per l'aggregato sulle cinque finestre. Sono intervalli descrittivi,
non IC 95%. sticky_06 è il null invariato; iid e sticky_09 sono cambiamenti.
Persino con 100 blocchi per calibrazione, la potenza mediana dell'aggregato contro
iid è zero. Nel doppio rango le code hanno una risoluzione finita e gli score
estremi possono saturare allo stesso valore; aggregare molte componenti e finestre
può annullare la capacità di segnalare certi cambiamenti. Il buon risultato con
1.000 blocchi non si trasferisce automaticamente a una storia corta.

Questo è un risultato negativo del candidato specifico e modifica la decisione:
**non adottare il suo aggregato empirico come percentuale finale per le strategie**.
La famiglia statistica resta appropriata al problema, ma va studiata una versione
più parsimoniosa, con score che conservino l'intensità nelle code e un fabbisogno
storico sostenibile, oppure un modello predittivo con assunzioni esplicite.
Tutti i risultati, compresi i casi con potenza zero, sono in
results/finite_history.csv e nella sintesi associata.

È un limite della procedura empirica adottata, non un teorema che servano secoli
per qualsiasi metodo. Modelli parsimoniosi possono condividere informazione fra
blocchi, ma guadagnano precisione al prezzo di assunzioni più forti da verificare.
La selezione del metodo deve tenere insieme memoria, disponibilità dello storico,
risoluzione della coda e costo dei falsi allarmi.

## 8. Protocollo proposto per il passaggio alle strategie reali

1. **Definire la variabile economica e il calendario.** Serie giornaliera ordinata
   di rendimento di portafoglio netto dei costi, aggiustata per versamenti/prelievi;
   dichiarare leva/esposizione, prezzi/NAV e sessioni di valutazione. Un trade non
   è automaticamente un'osservazione indipendente: più titoli nello stesso giorno
   condividono fattori. Una serie di PnL per trade risponde a un'altra domanda.
2. **Verificare il dato prima del test.** Zero rendimento realmente osservato,
   giorno senza trade e dato mancante non sono sinonimi. Posizioni overnight e
   valorizzazioni incomplete vanno trattate esplicitamente. Il confronto deve
   usare informazioni note alla data della valutazione; la maturazione degli
   esiti non può usare uscite future.
3. **Congelare una baseline precedente al blocco da valutare.** Per inferenza,
   le finestre recenti non devono entrare nel riferimento. L'attuale confronto UI
   sull'intero run, che le include, resta una descrizione retrospettiva con
   sovrapposizione; non certifica un test fuori campione. Salvare versione di
   strategia, dati, universo, costi e normalizzazione. Non unire configurazioni
   diverse come se fossero la stessa legge.
4. **Esplicitare H0 e il contesto.** Prestazione insolita rispetto alla storia
   complessiva, oppure rispetto a condizioni di mercato comparabili? Una
   strategia condizionalmente stabile può variare se cambia la distribuzione
   dei regimi. Aggiungere covariate richiede supporto storico sufficiente, non
   la selezione retrospettiva di un regime che renda innocuo l'allarme.
5. **Studiare una piccola famiglia di score prima del test finale.** Per rendimenti:
   composizione/distribuzione marginale; persistenza del segno; relazioni a più
   ritardi; clustering della volatilità; traiettoria cumulata/drawdown; errore o
   sorpresa di un modello predittivo semplice. Le caratteristiche dei dadi non
   si trasferiscono automaticamente. MMD su vettori di blocco è un candidato
   più ampio, da calibrare tenendo conto della dipendenza e della dimensione.
6. **Separare training, calibrazione e verifica cronologica**, con purge/gap coerenti
   con durata delle posizioni e memoria; nessuna soglia o scelta di score sui
   periodi riservati. Usare più epoche e baseline vintage per stimare quanto
   cambia la calibrazione. Dichiarare quanta storia è davvero indipendente o
   approssimativamente tale. Con B blocchi il rango superiore minimo è
   1/(B+1), e B blocchi sovrapposti non sono B repliche indipendenti.
7. **Valutare falsi allarmi e potenza su cambiamenti dichiarati.** Media,
   dispersione, code, persistenza, volatilità condizionale, cambio di esposizione
   e di esecuzione; includere cambiamenti lievi, non soltanto differenze forti.
   Simulazioni realistiche sono stress test del metodo, non prove sul mercato.
8. **Calibrare insieme finestre, score e ripetizione temporale.** Per la prima
   versione descrittiva si può esporre un rango puntuale con limiti espliciti.
   Per una regola di allarme, scegliere orizzonte e tolleranza ai falsi allarmi
   e valutarla su traiettorie intere. Aumentare la soglia dopo aver visto molti
   falsi allarmi non sostituisce una verifica separata.
9. **Separare rilevazione e diagnosi.** Un allarme porta a controllare dati e
   configurazione, esecuzione reale rispetto a replay comparabile, ed economia
   della strategia rispetto al contesto. Non autorizza automaticamente stop,
   cambio strategia o una diagnosi di crowding/perdita permanente di edge.

Una futura scheda statistica dovrebbe riportare: data/periodo del riferimento,
numero di sessioni complete, rango di anomalia del blocco e relativo ambito
puntuale/globale, componenti che contribuiscono, direzione economica separata,
supporto della calibrazione e stato «dati insufficienti» quando necessario.
Un valore alto significa **assenza di evidenza rilevata dagli score scelti**,
non «probabilità alta che il dado sia lo stesso».

## 9. Cosa resta da decidere e cosa è già verificato

Lo studio sintetico è concluso nel proprio scope: generatori uniformi dipendenti,
media iid, score marginali/temporali, aggregazione delle finestre, ripetizione,
incertezza della baseline e sensibilità al bootstrap sono stati eseguiti.

Per una procedura utilizzabile sulle strategie restano da scegliere la serie
primaria e la baseline storica comparabile, verificare la stazionarietà utile,
progettare score per rendimenti e fissare l'obiettivo del monitoraggio ripetuto.
Non si può ricavare dal solo esempio del dado una soglia universale né una
probabilità di permanenza della strategia. Non è stato eseguito alcun backtest
finanziario o esperimento sui risultati reali; non ci sono conclusioni su edge,
mercato, capitale o produzione.

## Riproducibilità e fonti

- [Notebook eseguito](studio.ipynb), con tabelle, figure e verifiche.
- [Protocollo prima delle simulazioni](protocol.md) e [addendum preventivo](protocol_addendum_before_execution.md).
- [Fonti primarie e confini](sources.md): stationary bootstrap, inferenza conforme
  sotto dipendenza, confidence sequence e MMD; i rispettivi risultati non sono
  attribuiti allo score sperimentale.
- results/rejection_rates.csv: tutte le 728 righe, comprese quelle senza potenza.
- inputs/: storico e blocchi recenti simulati, oltre allo snapshot del calcolo UI.
- results/manifest.json: seed, configurazione, versioni, risorse e hash.
- results/validation.json: controlli indipendenti. Intervalli Monte Carlo
  condizionati alla baseline, non una garanzia generale di copertura.

La probabilità analitica del dado è una derivazione del modello dichiarato.
La validità asintotica del bootstrap a blocchi è trattata da
[Politis e Romano](https://doi.org/10.1080/01621459.1994.10476870).
La distinzione fra scambiabilità esatta e serie dipendenti è esplicita in
[Chernozhukov, Wüthrich e Zhu](https://proceedings.mlr.press/v75/chernozhukov18a.html).
Il controllo uniforme nel tempo richiede condizioni proprie, come nelle
[confidence sequence di Howard et al.](https://arxiv.org/abs/1810.08240).
[MMD di Gretton et al.](https://www.jmlr.org/papers/v13/gretton12a.html) è qui un
candidato concettuale; non è stato sperimentato e il suo test iid non viene
presentato come valido automaticamente sui rendimenti dipendenti.
'''
    (ROOT / 'studio_statistico.md').write_text(text)


if __name__ == '__main__':
    write()
