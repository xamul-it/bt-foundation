# Fonti primarie e confini delle affermazioni

Consultazione web: 24 ottobre 2026. Nessuna fonte viene interpretata come garanzia
finita universale per il problema dell'utente.

1. Politis, D. N.; Romano, J. P. (1994), *The Stationary Bootstrap*, JASA 89,
   1303–1313. DOI https://doi.org/10.1080/01621459.1994.10476870
   Testo: https://users.ssc.wisc.edu/~behansen/718/Politis%20Romano.pdf
   Bootstrap con blocchi di lunghezza geometrica per processi stazionari debolmente
   dipendenti; supporto teorico asintotico. Non autorizza a resamplare singoli
   rendimenti indipendentemente o a scegliere qualunque lunghezza di blocco.

2. Chernozhukov, V.; Wüthrich, K.; Zhu, Y. (2018), *Exact and Robust Conformal
   Inference Methods for Predictive Machine Learning with Dependent Data*,
   COLT / PMLR 75, 732–749.
   https://proceedings.mlr.press/v75/chernozhukov18a.html
   https://proceedings.mlr.press/v75/chernozhukov18a/chernozhukov18a.pdf
   Le permutazioni a blocchi possono tenere conto della dipendenza. L'esattezza
   sotto scambiabilità non si trasferisce automaticamente alle serie temporali;
   il lavoro distingue validità esatta e approssimata. Lo score empirico di questo
   studio non implementa il metodo del paper e non ne eredita i teoremi.

3. Howard, S. R.; Ramdas, A.; McAuliffe, J.; Sekhon, J. (2021), *Time-uniform,
   nonparametric, nonasymptotic confidence sequences*, Annals of Statistics 49,
   1055–1080. https://doi.org/10.1214/20-AOS1991
   https://arxiv.org/abs/1810.08240
   Inferenza uniforme nel tempo richiede procedure e assunzioni specifiche,
   comprese condizioni di martingala o di concentrazione appropriate. Non basta
   ricalcolare ogni giorno un p-value valido a data fissa.

4. Gretton et al. (2012), *A Kernel Two-Sample Test*, JMLR 13, 723–773.
   https://www.jmlr.org/papers/v13/gretton12a.html
   https://www.jmlr.org/papers/volume13/gretton12a/gretton12a.pdf
   MMD offre un confronto distribuzionale più ricco; se applicato ai soli singoli
   rendimenti non ne considera l'ordine. Per dipendenza serve costruire blocchi
   e una calibrazione coerente; il test iid del paper non è trasferibile senza
   condizioni. Questo metodo viene valutato concettualmente, non sperimentato.

Derivazioni del modello dei dadi, scelta degli score, confronti Monte Carlo,
protocollo per strategie reali e raccomandazioni sono lavoro originale dello
studio. Le fonti motivano famiglie e limiti; non provano i risultati simulati.
