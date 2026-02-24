# bt-shell.sh — funzioni e prompt per il trading system
# USO: source /home/htpc/backtrader/scripts/bt-shell.sh
#      (NON eseguire direttamente: le funzioni resterebbero nel sottoprocesso)

_BT_ENV_DIR="/home/htpc/backtrader/env"
_BT_BASE_PS1="$PS1"

# Disabilita il prefisso (venv) automatico: lo gestiamo noi
VIRTUAL_ENV_DISABLE_PROMPT=1

# Ricostruisce il prompt con venv + profilo BT
_bt_update_ps1() {
    local venv_prefix="" bt_prefix=""
    if [[ -n "$VIRTUAL_ENV" ]]; then
        local venv_name
        venv_name="$(basename "$(dirname "$VIRTUAL_ENV")")"
        venv_prefix="\[\e[0;36m\](${venv_name})\[\e[0m\] "
    fi
    if [[ -n "$_BT_PROFILE" ]]; then
        bt_prefix="\[\e[1;33m\][${_BT_PROFILE}]\[\e[0m\] "
    fi
    PS1="${bt_prefix}${venv_prefix}${_BT_BASE_PS1}"
}
PROMPT_COMMAND="${PROMPT_COMMAND:+${PROMPT_COMMAND}; }_bt_update_ps1"

# bt-env: carica uno o più profili da $_BT_ENV_DIR ed esporta le variabili
# Uso: bt-env pa2 replay   → carica pa2 + replay, prompt [PA2+REPLAY]
#      bt-env pa2-replay   → profilo composto (equivalente)
#      bt-env              → mostra profili disponibili e profilo attivo
bt-env() {
    if [[ $# -eq 0 ]]; then
        echo "Uso: bt-env <profilo> [profilo2 ...]"
        echo ""
        echo "Profili disponibili in $_BT_ENV_DIR:"
        ls "$_BT_ENV_DIR" 2>/dev/null | column
        [[ -n "$_BT_PROFILE" ]] && echo "" && echo "Attivo: [$_BT_PROFILE]"
        return 0
    fi
    local labels=() p
    for p in "$@"; do
        local f="$_BT_ENV_DIR/$p"
        if [[ ! -f "$f" ]]; then
            echo "bt-env: profilo non trovato: $p (cerca in $_BT_ENV_DIR)" >&2
            return 1
        fi
        set -a
        # shellcheck source=/dev/null
        source "$f"
        set +a
        labels+=("${p^^}")
    done
    export _BT_PROFILE=$(IFS='+'; echo "${labels[*]}")
    echo "Attivo: [$_BT_PROFILE]"
}

# bt-env-clear: rimuove il profilo attivo e ripristina il prompt standard
bt-env-clear() {
    unset _BT_PROFILE
    echo "Profilo rimosso"
}
