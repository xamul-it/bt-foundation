"""Proposed source taxonomy. Document role and strategy context never imply validity."""
from pathlib import Path
import re

FAMILY_RULES = [
    ('OvernightAH', r'overnight[-_ ]?ah|(?:^|/)ah[_/-]|adaptive_cooldown|ah_cooldown'),
    ('RMA', r'(?:^|[/ _.-])rma(?:[/ _.-]|strategy|$)'),
    ('HMA', r'(?:^|[/ _.-])hma(?:[/ _.-]|dynamic|$)'),
    ('MultiFactor', r'multifactor|multi_factor'),
    ('RTH', r'(?:^|[/ _.-])rth(?:[/ _.-]|$)'),
    ('QuantConnect', r'quantconnect'),
    ('TechMomentum', r'tech[-_ ]?momentum'),
    ('Supertrend', r'supertrend|super_trend'),
    ('ORB', r'(?:^|/)orb(?:/|$)'),
    ('BinomialRegression', r'binomial_regression'),
]


REVIEWED_ROLES = {
    'AlpacaBroker Concurrency Notes': 'reference',
    'Intraday HMA - Flusso Ordini': 'reference',
    'HMADynamic Backtest - Issues Identified': 'review',
    'EXACT MATCH SUCCESS': 'report',
    'HMA Strategy - Position Sizing & Risk Management': 'guide',
    'Studio di eseguibilità intraday.HMA': 'plan',
    'Identità runtime development OvernightAH': 'review',
    "Orchestrazione dell'estensione": 'plan',
    'Correzione tecnica V2': 'review',
    'Large Cap Pullback Mean Reversion': 'reference',
    'Margin and Short Selling': 'reference',
    'Placing Orders': 'reference',
    'Strategia RTH: percorso agentico di ricerca e validazione': 'protocol',
    'RTH AH/Gap Candidate Runbook': 'guide',
    'Confronto development:': 'report',
    'Proposta: bilanciamento': 'plan',
    'Estensione dichiarata prima di eseguirla: capacità dello storico': 'report',
}


def strategy_families(context):
    return [family for family, pattern in FAMILY_RULES if re.search(pattern, context, re.I)]


def enrich_metadata(path, text, meta):
    result = dict(meta)
    result['evidence'] = list(meta.get('evidence', []))
    filename = Path(path).stem.lower()
    title = meta.get('title', '')
    role_text = filename + ' ' + title.lower()
    reason = None
    kind = None
    if meta.get('catalog_kind') == 'study_card': kind, reason = 'study_card', 'generated study navigation'
    elif filename == 'codex' and re.search(r'trace|conversation',title,re.I): kind, reason = 'reference', 'historical conversation trace'
    elif filename in {'claude', 'agents', 'codex'}: kind, reason = 'instructions', 'agent instruction filename'
    elif filename == 'readme': kind, reason = 'index', 'README entry point'
    elif re.search(r'/plans/|outline|\bplan\b|_plan|piano|brief|questions|prompts|request|hypothesis|ipotesi', path.lower() + ' ' + role_text): kind, reason = 'plan', 'planning role in path/title'
    elif re.search(r'protocol|protocoll|precommitted', role_text): kind, reason = 'protocol', 'protocol role in path/title'
    elif re.search(r'contract|contratto|methodolog|metodolog|semantics|fonti|sources|inventory|inventario|register|registro|blueprint|architecture|context|_notes|guardrails', role_text): kind, reason = 'reference', 'technical/reference role in path/title'
    elif re.search(r'review|audit|checkpoint|assessment|verifica|verification', role_text): kind, reason = 'review', 'review role in path/title'
    elif re.search(r'decision|director|ruling|amendment|clarification', role_text): kind, reason = 'decision', 'governance role in path/title'
    elif re.search(r'report|result|risultat|conclus|summary|sintesi|analysis|analisi|findings|recommendation|raccomand|reconciliation|disposition|readout|final_status|feasibility|sensitiv|calibration|calibraz|_final', role_text): kind, reason = 'report', 'analysis/results role in path/title'
    elif re.search(r'guide|guida|quick_start|readme_|implementation|implementazione|_fix|handling|explained|usage|example|options', role_text): kind, reason = 'guide', 'usage/implementation role in path/title'
    elif '/docs/' in path or path.startswith('docs/'): kind, reason = 'guide', 'documentation directory'
    reviewed = next(((quote, role) for quote,role in REVIEWED_ROLES.items() if quote in title), None)
    if reviewed and not kind:
        kind, reason = reviewed[1], 'Document role reviewed from title and introductory source text'
    if kind:
        result['evidence'] = [e for e in result['evidence'] if e.get('field') != 'doc_type']
        result['doc_type'] = kind
        result['evidence'].append({'field': 'doc_type', 'value': kind, 'method': 'proposed_path_title_rule', 'basis': path, 'reason': reason})
    if reviewed:
        result['evidence'].append({'field':'doc_type','value':kind,'method':'curated_document_role','quote':reviewed[0],'line':next(i for i,line in enumerate(text.splitlines(),1) if reviewed[0] in line)})
        result['metadata_state']='mixed_curated_and_proposed'
    result['classification_reason'] = reason or 'No unambiguous document-role rule matched; needs review'
    # Frozen application docs retain their own component context, not the enclosing experiment family.
    context_path = path.split('/frozen_development/', 1)[-1]
    families = strategy_families(context_path + ' ' + title)
    if not families and meta.get('catalog_kind') == 'study_card':
        families = strategy_families(meta.get('study_id', ''))
    result['strategy_families'] = families
    result['strategy_scope'] = 'identified' if families else 'cross_cutting'
    if families:
        result['evidence'].append({'field': 'strategy_families', 'value': families,
                                  'method': 'proposed_path_title_context', 'basis': context_path, 'quote': title})
    return result
