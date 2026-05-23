import os
import asyncio
import logging
import re
import json
import aiosqlite
from collections import defaultdict
from time import time
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.filters import Command
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, BotCommand
from aiohttp import web
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.base import BaseStorage, StorageKey, StateType
from aiogram.fsm.storage.memory import MemoryStorage
from database import (
    init_db,
    is_first_visit,
    registrar_gasto,
    deletar_gasto,
    get_gastos_recentes,
    registrar_receita,
    registrar_investimento,
    registrar_recebimento,
    get_gastos_periodo,
    get_receitas_periodo,
    get_investimentos_periodo,
    get_rank_gastos,
    get_media_utilidade,
    get_media_utilidade_geral,
    get_pendentes,
    marcar_recebido,
    get_investimentos,
    deletar_investimento,
    editar_investimento,
    get_investimento_by_id,
    registrar_uso_comando,
    get_comandos_mais_usados,
    definir_meta,
    get_meta,
    definir_meta_investimento,
    get_meta_investimento,
    definir_meta_inv_data,
    get_meta_inv_data,
    definir_meta_receita,
    get_meta_receita,
    get_stats_globais,
)

# ─── Rate Limiting ────────────────────────────────────────────────────────────

_rate: dict[int, list] = defaultdict(list)
RATE_WINDOW = 10   # segundos
RATE_MAX    = 15   # mensagens por janela


def is_rate_limited(user_id: int) -> bool:
    """Limita mensagens por usuário para evitar spam."""
    now = time()
    _rate[user_id] = [t for t in _rate[user_id] if now - t < RATE_WINDOW]
    if len(_rate[user_id]) >= RATE_MAX:
        return True
    _rate[user_id].append(now)
    # Limpa entradas de usuários inativos para não vazar memória
    inativos = [uid for uid, ts in _rate.items() if not ts]
    for uid in inativos:
        del _rate[uid]
    return False


# ─── Token Filter ─────────────────────────────────────────────────────────────

class TokenFilter(logging.Filter):
    """Mascara o token do bot em todos os logs."""
    def __init__(self, token: str = ""):
        self.token = token

    def filter(self, record):
        if self.token:
            record.msg = str(record.msg).replace(self.token, "***TOKEN***")
        return True


logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

ADMIN_ID = int(os.environ.get("ADMIN_ID", 0))
_bot_ref = None


async def notificar_admin(texto: str) -> None:
    """Envia mensagem ao admin."""
    if _bot_ref is None or not ADMIN_ID:
        return
    try:
        await _bot_ref.send_message(ADMIN_ID, texto, parse_mode="MarkdownV2")
    except Exception as e:
        logger.warning("Falha ao notificar admin: %s", e)


async def notificar_novo_usuario(user) -> None:
    """Dispara quando novo usuário inicia o bot."""
    stats = get_stats_globais()
    nome = md(user.full_name or user.first_name or 'Desconhecido')
    at = md('@' + user.username) if user.username else '_sem username_'
    linhas = [
        '*Novo usuario\\!*',
        '',
        'Nome: *' + nome + '* \\(' + at + '\\)',
        'ID: `' + str(user.id) + '`',
        '',
        'Total de usuarios: *' + str(stats['total_usuarios']) + '*',
    ]
    await notificar_admin('\n'.join(linhas))


async def notificar_rate_limit(user) -> None:
    """Dispara quando rate limit é acionado."""
    nome = md(user.full_name or user.first_name or 'Desconhecido')
    at = md('@' + user.username) if user.username else '_sem username_'
    linhas = [
        '*Rate limit ativado*',
        '',
        'Usuario: *' + nome + '* \\(' + at + '\\)',
        'ID: `' + str(user.id) + '`',
        '',
        '_Muitas mensagens em curto intervalo\\._',
    ]
    await notificar_admin('\n'.join(linhas))


async def relatorio_diario() -> None:
    """Relatório diário para o admin."""
    stats = get_stats_globais()
    vol = md(formatar_valor(stats['volume_gastos']))
    linhas = [
        '*Relatorio Diario \\- Minerva Finance*',
        '',
        'Usuarios cadastrados: *' + str(stats['total_usuarios']) + '*',
        'Ativos hoje: *' + str(stats['usuarios_ativos_hoje']) + '*',
        '',
        'Gastos registrados \\(total\\): *' + str(stats['total_gastos']) + '*',
        'Receitas registradas: *' + str(stats['total_receitas']) + '*',
        'Investimentos: *' + str(stats['total_investimentos']) + '*',
        '',
        'Volume total de gastos: *' + vol + '*',
        'Novos gastos hoje: *' + str(stats['gastos_hoje']) + '*',
    ]
    await notificar_admin('\n'.join(linhas))


async def agendar_relatorio_diario() -> None:
    """Envia relatório todo dia às 8h."""
    while True:
        agora = datetime.now()
        proximo = agora.replace(hour=8, minute=0, second=0, microsecond=0)
        if proximo <= agora:
            proximo += timedelta(days=1)
        await asyncio.sleep((proximo - agora).total_seconds())
        await relatorio_diario()


router = Router()


# ─── SQLite FSM Storage ───────────────────────────────────────────────────────

class SqliteStorage(BaseStorage):
    def __init__(self, db_path: str = "fsm_storage.db"):
        self.db_path = db_path

    async def _init(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""CREATE TABLE IF NOT EXISTS fsm (
                key TEXT PRIMARY KEY, state TEXT, data TEXT
            )""")
            await db.commit()

    async def set_state(self, key: StorageKey, state: StateType = None):
        await self._init()
        k = f"{key.bot_id}:{key.chat_id}:{key.user_id}"
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO fsm(key,state) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET state=excluded.state",
                (k, state.state if state else None))
            await db.commit()

    async def get_state(self, key: StorageKey) -> str | None:
        await self._init()
        k = f"{key.bot_id}:{key.chat_id}:{key.user_id}"
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT state FROM fsm WHERE key=?", (k,)) as cur:
                row = await cur.fetchone()
                return row[0] if row else None

    async def set_data(self, key: StorageKey, data: dict):
        await self._init()
        k = f"{key.bot_id}:{key.chat_id}:{key.user_id}"
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO fsm(key,data) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET data=excluded.data",
                (k, json.dumps(data)))
            await db.commit()

    async def get_data(self, key: StorageKey) -> dict:
        await self._init()
        k = f"{key.bot_id}:{key.chat_id}:{key.user_id}"
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT data FROM fsm WHERE key=?", (k,)) as cur:
                row = await cur.fetchone()
                return json.loads(row[0]) if row and row[0] else {}

    async def close(self):
        pass


# ─── States ───────────────────────────────────────────────────────────────────

class GastoStates(StatesGroup):
    aguardando_nome = State()
    aguardando_valor = State()
    aguardando_utilidade = State()
    aguardando_confirmar_deletar = State()

class ReceitaStates(StatesGroup):
    aguardando_nome = State()
    aguardando_valor = State()

class InvestimentoStates(StatesGroup):
    aguardando_nome = State()
    aguardando_valor = State()

class ReceberStates(StatesGroup):
    aguardando_nome = State()
    aguardando_valor = State()
    aguardando_data = State()

class EditarInvStates(StatesGroup):
    aguardando_id = State()
    aguardando_nome = State()
    aguardando_valor = State()

class DeletarInvStates(StatesGroup):
    aguardando_id = State()
    aguardando_confirmacao = State()

class DeletarGastoStates(StatesGroup):
    aguardando_id = State()
    aguardando_confirmacao = State()

class MetaStates(StatesGroup):
    aguardando_tipo = State()
    aguardando_subtipo_inv = State()
    aguardando_valor = State()
    aguardando_data_inv = State()


# ─── Helpers ──────────────────────────────────────────────────────────────────

ESTRELAS = ["⭐", "⭐⭐", "⭐⭐⭐", "⭐⭐⭐⭐", "⭐⭐⭐⭐⭐"]

UTILIDADE_DESC = (
    "0 — 🗑️ Desnecessário \\(não precisava ter comprado\\)\n"
    "1 — 😐 Quase inútil \\(uso muito raramente\\)\n"
    "2 — 🤔 Mediano \\(uso às vezes, mas não faz falta\\)\n"
    "3 — 👍 Útil \\(uso com frequência e vale a pena\\)\n"
    "4 — 😊 Muito útil \\(uso bastante e facilitou minha vida\\)\n"
    "5 — 🏆 Essencial \\(uso sempre, não vivo sem\\)"
)

COMANDOS_PADRAO = ["/gasto", "/receita", "/investimento", "/receber", "/resumo mes", "/pendentes"]
TODOS_COMANDOS = [
    "/gasto", "/receita", "/investimento", "/receber",
    "/resumo mes", "/resumo semana", "/resumo ano",
    "/pendentes", "/rank", "/utilidade",
    "/meta", "/meus_investimentos",
    "/deletar_gasto", "/editar_investimento", "/deletar_investimento",
]

FSM_TIMEOUT = timedelta(minutes=30)


def formatar_valor(valor):
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def parse_valor(s: str) -> float:
    """Rejeita valores acima de R$ 10 milhões por registro."""
    s = s.strip()
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")
    valor = float(s)
    if valor > 10_000_000:
        raise ValueError("valor acima do limite permitido de R$ 10.000.000")
    return valor


def md(text: str) -> str:
    for ch in r"\_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text


def get_body(text: str) -> str:
    parts = text.split(None, 1)
    return parts[1].strip() if len(parts) > 1 else ""


def build_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    mais_usados = get_comandos_mais_usados(user_id, limit=6)
    fila = list(mais_usados)
    for cmd in COMANDOS_PADRAO:
        if cmd not in fila:
            fila.append(cmd)
    principais = fila[:6]
    extras = [c for c in TODOS_COMANDOS if c not in principais]
    linhas = [
        [KeyboardButton(text=c) for c in principais[:3]],
        [KeyboardButton(text=c) for c in principais[3:6]],
    ]
    if extras:
        linhas.append([KeyboardButton(text=c) for c in extras[:3]])
    if len(extras) > 3:
        linhas.append([KeyboardButton(text=c) for c in extras[3:6]])
    return ReplyKeyboardMarkup(keyboard=linhas, resize_keyboard=True,
                               input_field_placeholder="Escolha ou digite um comando...")


def alerta_meta(user_id: int, total_mes: float) -> str:
    meta = get_meta(user_id)
    if not meta:
        return ""
    pct = (total_mes / meta) * 100
    barra_cheia = int(pct / 10)
    barra = "█" * min(barra_cheia, 10) + "░" * max(0, 10 - barra_cheia)
    cor = "🟢" if pct < 70 else ("🟡" if pct < 90 else "🔴")
    aviso = ""
    if pct >= 100:
        aviso = "\n⚠️ *Meta mensal ultrapassada\\!*"
    elif pct >= 90:
        aviso = "\n⚠️ *Atenção: 90% da meta atingida\\!*"
    return (
        f"\n━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 *Meta do mês:* {md(formatar_valor(meta))}\n"
        f"{cor} `{barra}` {md(f'{pct:.1f}')}%{aviso}\n"
    )


def alerta_meta_investimento(user_id: int, total_acumulado: float) -> str:
    # Tenta meta por data primeiro
    meta_data = get_meta_inv_data(user_id)
    if meta_data:
        meta_val, prazo_str = meta_data
        pct = (total_acumulado / meta_val) * 100
        barra_cheia = int(pct / 10)
        barra = "█" * min(barra_cheia, 10) + "░" * max(0, 10 - barra_cheia)
        cor = "🟢" if pct >= 100 else ("🟡" if pct >= 70 else "🔴")
        aviso = ""
        if pct >= 100:
            aviso = "\n✅ *Meta de investimento atingida\\!*"
        elif pct >= 70:
            aviso = "\n📈 *Quase lá: 70% da meta\\!*"
        try:
            prazo_fmt = datetime.strptime(prazo_str, "%Y-%m-%d").strftime("%d/%m/%Y")
            hoje = datetime.now().date()
            dias = (datetime.strptime(prazo_str, "%Y-%m-%d").date() - hoje).days
            dias_txt = f" \\({dias}d restantes\\)" if dias > 0 else " \\(prazo encerrado\\)"
        except Exception:
            prazo_fmt = prazo_str
            dias_txt = ""
        return (
            f"\n━━━━━━━━━━━━━━━━━━━━━\n"
            f"📈 *Meta invest\\. até {md(prazo_fmt)}*{dias_txt}\n"
            f"🎯 {md(formatar_valor(total_acumulado))} / {md(formatar_valor(meta_val))}\n"
            f"{cor} `{barra}` {md(f'{pct:.1f}')}%{aviso}\n"
        )
    # Senão tenta meta mensal
    meta = get_meta_investimento(user_id)
    if not meta:
        return ""
    pct = (total_acumulado / meta) * 100
    barra_cheia = int(pct / 10)
    barra = "█" * min(barra_cheia, 10) + "░" * max(0, 10 - barra_cheia)
    cor = "🟢" if pct >= 100 else ("🟡" if pct >= 70 else "🔴")
    aviso = ""
    if pct >= 100:
        aviso = "\n✅ *Meta de investimento atingida\\!*"
    elif pct >= 70:
        aviso = "\n📈 *Quase lá: 70% da meta de investimento\\!*"
    return (
        f"\n━━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 *Meta invest\\..:* {md(formatar_valor(meta))}\n"
        f"{cor} `{barra}` {md(f'{pct:.1f}')}%{aviso}\n"
    )


def alerta_meta_receita(user_id: int, total_mes: float) -> str:
    meta = get_meta_receita(user_id)
    if not meta:
        return ""
    pct = (total_mes / meta) * 100
    barra_cheia = int(pct / 10)
    barra = "█" * min(barra_cheia, 10) + "░" * max(0, 10 - barra_cheia)
    cor = "🟢" if pct >= 100 else ("🟡" if pct >= 70 else "🔴")
    aviso = ""
    if pct >= 100:
        aviso = "\n✅ *Meta de receita atingida\\!*"
    elif pct >= 70:
        aviso = "\n💰 *Quase lá: 70% da meta de receita\\!*"
    return (
        f"\n━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 *Meta de receita:* {md(formatar_valor(meta))}\n"
        f"{cor} `{barra}` {md(f'{pct:.1f}')}%{aviso}\n"
    )


# ─── Textos fixos ─────────────────────────────────────────────────────────────

INTRO = (
    "👋 *Olá\\! Eu sou a Minerva* 💎\n"
    "Seu assistente financeiro pessoal aqui no Telegram\\.\n\n"
    "━━━━━━━━━━━━━━━━━━━━━\n"
    "✅ *O que você pode fazer comigo:*\n\n"
    "💸 Registrar gastos com nota de utilidade\n"
    "💰 Registrar receitas e investimentos\n"
    "📅 Guardar valores que ainda vai receber\n"
    "📊 Ver relatórios semanais, mensais e anuais\n"
    "🎯 Definir meta de gastos, investimentos e receita mensal com alerta\n"
    "🏆 Ranking e análise de utilidade dos gastos\n\n"
    "━━━━━━━━━━━━━━━━━━━━━\n"
    "🚀 *Pronto para começar\\?*\n\n"
    "Digite /gasto para registrar seu primeiro gasto\\!\n\n"
    "_Use /ajuda para ver todos os comandos\\._"
)

AJUDA = (
    "📖 *Minerva Finance — Comandos*\n\n"
    "━━━━━━━━━━━━━━━━━━━━━\n"
    "📤 `/gasto` — Registrar gasto\n"
    "💰 `/receita` — Registrar receita\n"
    "📈 `/investimento` — Registrar investimento\n"
    "💵 `/receber` — Registrar valor a receber\n"
    "🗑️ `/deletar_gasto` — Excluir um gasto\n\n"
    "━━━━━━━━━━━━━━━━━━━━━\n"
    "🎯 `/meta` — Definir metas mensais \\(gastos, invest\\., receita\\)\n"
    "📊 `/resumo semana` `/resumo mes` `/resumo ano`\n"
    "🏆 `/rank` — Gastos por valor\n"
    "⭐ `/utilidade` — Análise de utilidade\n"
    "💵 `/pendentes` — A receber em aberto\n"
    "✅ `/recebido ID` — Marcar como recebido\n\n"
    "━━━━━━━━━━━━━━━━━━━━━\n"
    "📋 `/meus_investimentos` — Listar\n"
    "✏️ `/editar_investimento` — Editar\n"
    "🗑️ `/deletar_investimento` — Excluir\n\n"
    "❌ `/cancelar` — Cancelar operação atual"
)


# ─── /start e /ajuda ──────────────────────────────────────────────────────────

@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    primeiro = is_first_visit(user_id)
    kb = build_keyboard(user_id)
    if primeiro:
        await message.answer(INTRO, parse_mode="MarkdownV2", reply_markup=kb)
        asyncio.create_task(notificar_novo_usuario(message.from_user))
    else:
        nome = message.from_user.first_name or "você"
        await message.answer(
            f"👋 Olá de novo, *{md(nome)}*\\!\n\nPronto para registrar algo?\n\n_Use /ajuda para ver os comandos\\._",
            parse_mode="MarkdownV2", reply_markup=kb)


@router.message(Command("ajuda"))
async def cmd_ajuda(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(AJUDA, parse_mode="MarkdownV2")


@router.message(Command("cancelar"))
async def cmd_cancelar(message: Message, state: FSMContext):
    current = await state.get_state()
    await state.clear()
    if current:
        await message.answer("❌ Operação cancelada\\.", parse_mode="MarkdownV2",
                             reply_markup=build_keyboard(message.from_user.id))
    else:
        await message.answer("Nenhuma operação em andamento\\.", parse_mode="MarkdownV2")


# ─── /meta ────────────────────────────────────────────────────────────────────

@router.message(Command("meta"))
async def cmd_meta(message: Message, state: FSMContext):
    if is_rate_limited(message.from_user.id):
        await message.answer("⚠️ Devagar\\! Muitas mensagens em pouco tempo\\.", parse_mode="MarkdownV2")
        asyncio.create_task(notificar_rate_limit(message.from_user))
        return
    await state.clear()
    registrar_uso_comando(message.from_user.id, "/meta")
    user_id = message.from_user.id
    meta_g = get_meta(user_id)
    meta_i = get_meta_investimento(user_id)
    meta_id = get_meta_inv_data(user_id)
    meta_r = get_meta_receita(user_id)

    linhas = []
    if meta_g:
        linhas.append(f"💸 Gastos: *{md(formatar_valor(meta_g))}*/mês")
    if meta_i:
        linhas.append(f"📈 Invest\\. mensal: *{md(formatar_valor(meta_i))}*/mês")
    if meta_id:
        prazo_fmt = md(datetime.strptime(meta_id[1], "%Y-%m-%d").strftime("%d/%m/%Y"))
        linhas.append(f"📈 Invest\\. por data: *{md(formatar_valor(meta_id[0]))}* até {prazo_fmt}")
    if meta_r:
        linhas.append(f"💰 Receita: *{md(formatar_valor(meta_r))}*/mês")
    atual = ("\n".join(linhas) + "\n\n") if linhas else ""

    await state.set_state(MetaStates.aguardando_tipo)
    await message.answer(
        f"🎯 *Definir Meta Mensal*\n\n{atual}"
        "Qual meta deseja definir\\?\n\n"
        "1️⃣ *Gastos* — limite máximo de gastos\n"
        "2️⃣ *Investimentos* — meta de investimento\n"
        "3️⃣ *Receita* — meta de dinheiro a receber\n\n"
        "_Digite 1, 2 ou 3_\n\n_/cancelar para sair_",
        parse_mode="MarkdownV2")


@router.message(MetaStates.aguardando_tipo)
async def meta_tipo(message: Message, state: FSMContext):
    opcao = message.text.strip()
    mapa = {"1": "gastos", "2": "investimentos", "3": "receita"}
    if opcao not in mapa:
        await message.answer("❌ Opção inválida\\. Digite *1*, *2* ou *3*\\.", parse_mode="MarkdownV2")
        return
    tipo = mapa[opcao]
    await state.update_data(tipo=tipo)

    if tipo == "investimentos":
        await state.set_state(MetaStates.aguardando_subtipo_inv)
        await message.answer(
            "📈 *Meta de Investimentos*\n\n"
            "Como deseja definir a meta\\?\n\n"
            "1️⃣ *Mensal* — valor mínimo por mês\n"
            "2️⃣ *Por data* — valor total a acumular até uma data\n\n"
            "_Digite 1 ou 2_\n\n_/cancelar para sair_",
            parse_mode="MarkdownV2")
        return

    nomes = {"gastos": "Gastos", "receita": "Receita"}
    await state.set_state(MetaStates.aguardando_valor)
    await message.answer(
        f"🎯 *Meta de {nomes[tipo]}*\n\n"
        "Qual o valor mensal\\?\n"
        "_Ex: 2000 ou 3500,00_\n\n_/cancelar para sair_",
        parse_mode="MarkdownV2")


@router.message(MetaStates.aguardando_subtipo_inv)
async def meta_subtipo_inv(message: Message, state: FSMContext):
    opcao = message.text.strip()
    if opcao not in ("1", "2"):
        await message.answer("❌ Opção inválida\\. Digite *1* ou *2*\\.", parse_mode="MarkdownV2")
        return
    subtipo = "mensal" if opcao == "1" else "data"
    await state.update_data(subtipo_inv=subtipo)
    await state.set_state(MetaStates.aguardando_valor)
    if subtipo == "mensal":
        await message.answer(
            "📈 *Meta de Investimentos Mensal*\n\n"
            "Qual o valor mínimo que deseja investir por mês\\?\n"
            "_Ex: 500 ou 1200,00_\n\n_/cancelar para sair_",
            parse_mode="MarkdownV2")
    else:
        await message.answer(
            "📈 *Meta de Investimentos por Data*\n\n"
            "Qual o valor total que deseja acumular\\?\n"
            "_Ex: 10000 ou 25000,00_\n\n_/cancelar para sair_",
            parse_mode="MarkdownV2")


@router.message(MetaStates.aguardando_valor)
async def meta_valor(message: Message, state: FSMContext):
    try:
        valor = parse_valor(message.text)
        if valor <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Valor inválido\\. Ex: `2000` ou `3500,00`", parse_mode="MarkdownV2")
        return
    data = await state.get_data()
    tipo = data.get("tipo", "gastos")
    subtipo_inv = data.get("subtipo_inv", "mensal")

    if tipo == "investimentos" and subtipo_inv == "data":
        await state.update_data(valor_inv_data=valor)
        await state.set_state(MetaStates.aguardando_data_inv)
        await message.answer(
            "📅 *Qual é o prazo\\?*\n\n"
            "Digite a data limite no formato *DD/MM/AAAA*\n"
            "_Ex: 31/12/2026_\n\n_/cancelar para sair_",
            parse_mode="MarkdownV2")
        return

    user_id = message.from_user.id
    nomes = {"gastos": "Gastos", "investimentos": "Investimentos", "receita": "Receita"}
    emojis = {"gastos": "💸", "investimentos": "📈", "receita": "💰"}
    desc = {
        "gastos": "_Você será avisado ao atingir 90% e 100% da meta\\._",
        "investimentos": "_Você será avisado ao atingir 70% e 100% da meta\\._",
        "receita": "_Você será avisado ao atingir 70% e 100% da meta\\._",
    }

    if tipo == "gastos":
        definir_meta(user_id, valor)
    elif tipo == "investimentos":
        definir_meta_investimento(user_id, valor)
    else:
        definir_meta_receita(user_id, valor)

    await state.clear()
    await message.answer(
        f"✅ *Meta de {nomes[tipo]} definida\\!*\n\n"
        f"{emojis[tipo]} {md(formatar_valor(valor))} por mês\n\n"
        f"{desc[tipo]}",
        parse_mode="MarkdownV2", reply_markup=build_keyboard(message.from_user.id))


@router.message(MetaStates.aguardando_data_inv)
async def meta_data_inv(message: Message, state: FSMContext):
    texto = message.text.strip()
    try:
        dt = datetime.strptime(texto, "%d/%m/%Y")
        if dt.date() <= datetime.now().date():
            raise ValueError("data no passado")
        prazo_iso = dt.strftime("%Y-%m-%d")
    except ValueError:
        await message.answer(
            "❌ Data inválida\\. Use o formato *DD/MM/AAAA* e uma data futura\\.\n"
            "_Ex: 31/12/2026_",
            parse_mode="MarkdownV2")
        return
    data = await state.get_data()
    valor = data["valor_inv_data"]
    user_id = message.from_user.id
    definir_meta_inv_data(user_id, valor, prazo_iso)
    await state.clear()
    prazo_fmt = md(dt.strftime("%d/%m/%Y"))
    hoje = datetime.now().date()
    dias = (dt.date() - hoje).days
    await message.answer(
        f"✅ *Meta de Investimentos por Data definida\\!*\n\n"
        f"🎯 *Valor:* {md(formatar_valor(valor))}\n"
        f"📅 *Prazo:* {prazo_fmt} \\({dias} dias\\)\n\n"
        "_Você será avisado ao atingir 70% e 100% da meta\\._",
        parse_mode="MarkdownV2", reply_markup=build_keyboard(user_id))


# ─── /gasto ───────────────────────────────────────────────────────────────────

@router.message(Command("gasto"))
async def cmd_gasto(message: Message, state: FSMContext):
    if is_rate_limited(message.from_user.id):
        await message.answer("⚠️ Devagar\\! Muitas mensagens em pouco tempo\\.", parse_mode="MarkdownV2")
        asyncio.create_task(notificar_rate_limit(message.from_user))
        return
    await state.clear()
    registrar_uso_comando(message.from_user.id, "/gasto")
    await state.set_state(GastoStates.aguardando_nome)
    await message.answer(
        "💸 *Registrar Gasto*\n\nCom o que foi o gasto\\?\n"
        "_Ex: Almoço, Uber, Netflix_\n\n_/cancelar para sair_",
        parse_mode="MarkdownV2")


@router.message(GastoStates.aguardando_nome)
async def gasto_nome(message: Message, state: FSMContext):
    nome = message.text.strip()
    if len(nome) > 100:
        await message.answer("❌ Nome muito longo\\. Use até 100 caracteres\\.", parse_mode="MarkdownV2")
        return
    await state.update_data(nome=nome)
    await state.set_state(GastoStates.aguardando_valor)
    await message.answer(
        f"📌 *{md(nome)}*\n\nQual foi o valor em reais\\?\n_Ex: 35,90 ou 145\\.81_",
        parse_mode="MarkdownV2")


@router.message(GastoStates.aguardando_valor)
async def gasto_valor(message: Message, state: FSMContext):
    try:
        valor = parse_valor(message.text)
        if valor <= 0:
            raise ValueError
    except ValueError:
        await message.answer(f"❌ Valor inválido: `{md(message.text.strip())}`\n\nUse: `35,90` ou `35.90`",
                             parse_mode="MarkdownV2")
        return
    await state.update_data(valor=valor)
    await state.set_state(GastoStates.aguardando_utilidade)
    await message.answer(
        f"🎯 *Qual a utilidade desse gasto?*\n\n{UTILIDADE_DESC}\n\nResponda com um número de *0 a 5*:",
        parse_mode="MarkdownV2")


@router.message(GastoStates.aguardando_utilidade)
async def gasto_utilidade(message: Message, state: FSMContext):
    try:
        utilidade = int(message.text.strip())
        if not 0 <= utilidade <= 5:
            raise ValueError
    except ValueError:
        await message.answer("❌ Use um número de *0* a *5*\\.", parse_mode="MarkdownV2")
        return
    data = await state.get_data()
    nome, valor = data["nome"], data["valor"]
    user_id = message.from_user.id
    registrar_gasto(user_id, nome, valor, utilidade)
    estrelas = ESTRELAS[utilidade - 1] if utilidade > 0 else "☆ Sem utilidade"

    hoje = datetime.now().date()
    inicio_mes = hoje.replace(day=1)
    gastos_mes = get_gastos_periodo(user_id, inicio_mes.isoformat(), hoje.isoformat())
    total_mes = sum(g["valor"] for g in gastos_mes)
    meta_txt = alerta_meta(user_id, total_mes)

    await state.clear()
    await message.answer(
        f"✅ *Gasto registrado\\!*\n\n📌 {md(nome)}\n💸 {md(formatar_valor(valor))}\n🎯 {utilidade}/5 {estrelas}{meta_txt}",
        parse_mode="MarkdownV2", reply_markup=build_keyboard(user_id))


# ─── /deletar_gasto ───────────────────────────────────────────────────────────

@router.message(Command("deletar_gasto"))
async def cmd_deletar_gasto(message: Message, state: FSMContext):
    if is_rate_limited(message.from_user.id):
        await message.answer("⚠️ Devagar\\! Muitas mensagens em pouco tempo\\.", parse_mode="MarkdownV2")
        asyncio.create_task(notificar_rate_limit(message.from_user))
        return
    await state.clear()
    user_id = message.from_user.id
    registrar_uso_comando(user_id, "/deletar_gasto")
    dados = get_gastos_recentes(user_id, 10)
    if not dados:
        await message.answer("📊 Nenhum gasto registrado ainda\\.", parse_mode="MarkdownV2")
        return
    msg = "🗑️ *Deletar Gasto*\n\nGastos recentes:\n\n"
    for g in dados:
        estrelas = ESTRELAS[g["utilidade"] - 1] if g["utilidade"] > 0 else "☆"
        data_reg = g["data_registro"][:10]
        msg += f"🔹 \\#{g['id']} *{md(g['nome'])}* — {md(formatar_valor(g['valor']))} {estrelas} _{md(data_reg)}_\n"
    msg += "\nQual o *ID* do gasto que deseja excluir?\n\n_/cancelar para sair_"
    await state.set_state(DeletarGastoStates.aguardando_id)
    await message.answer(msg, parse_mode="MarkdownV2")


@router.message(DeletarGastoStates.aguardando_id)
async def deletar_gasto_id(message: Message, state: FSMContext):
    try:
        gasto_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ ID inválido\\. Informe apenas o número\\.", parse_mode="MarkdownV2")
        return
    user_id = message.from_user.id
    dados = get_gastos_recentes(user_id, 50)
    gasto = next((g for g in dados if g["id"] == gasto_id), None)
    if not gasto:
        await message.answer(f"❌ Gasto \\#{gasto_id} não encontrado\\.\nUse /deletar\\_gasto para ver os IDs\\.",
                             parse_mode="MarkdownV2")
        return
    estrelas = ESTRELAS[gasto["utilidade"] - 1] if gasto["utilidade"] > 0 else "☆"
    await state.update_data(gasto_id=gasto_id)
    await state.set_state(DeletarGastoStates.aguardando_confirmacao)
    await message.answer(
        f"⚠️ *Confirmar exclusão?*\n\n"
        f"📌 {md(gasto['nome'])} — {md(formatar_valor(gasto['valor']))} {estrelas}\n\n"
        "Digite *sim* para confirmar ou /cancelar para sair:",
        parse_mode="MarkdownV2")


@router.message(DeletarGastoStates.aguardando_confirmacao)
async def deletar_gasto_confirmar(message: Message, state: FSMContext):
    if message.text.strip().lower() != "sim":
        await message.answer("Operação cancelada\\.", parse_mode="MarkdownV2",
                             reply_markup=build_keyboard(message.from_user.id))
        await state.clear()
        return
    data = await state.get_data()
    gasto_id = data["gasto_id"]
    user_id = message.from_user.id
    row = deletar_gasto(user_id, gasto_id)
    await state.clear()
    if row:
        await message.answer(f"🗑️ *Gasto \\#{gasto_id} excluído\\!*\n\n📌 {md(row['nome'])} — {md(formatar_valor(row['valor']))}",
                             parse_mode="MarkdownV2", reply_markup=build_keyboard(user_id))
    else:
        await message.answer("❌ Não foi possível excluir\\.", parse_mode="MarkdownV2")


# ─── /receita ─────────────────────────────────────────────────────────────────

@router.message(Command("receita"))
async def cmd_receita(message: Message, state: FSMContext):
    if is_rate_limited(message.from_user.id):
        await message.answer("⚠️ Devagar\\! Muitas mensagens em pouco tempo\\.", parse_mode="MarkdownV2")
        asyncio.create_task(notificar_rate_limit(message.from_user))
        return
    await state.clear()
    registrar_uso_comando(message.from_user.id, "/receita")
    await state.set_state(ReceitaStates.aguardando_nome)
    await message.answer(
        "💰 *Registrar Receita*\n\nQual foi a origem da receita\\?\n"
        "_Ex: Salário, Freela, Venda_\n\n_/cancelar para sair_",
        parse_mode="MarkdownV2")


@router.message(ReceitaStates.aguardando_nome)
async def receita_nome(message: Message, state: FSMContext):
    nome = message.text.strip()
    if len(nome) > 100:
        await message.answer("❌ Nome muito longo\\. Use até 100 caracteres\\.", parse_mode="MarkdownV2")
        return
    await state.update_data(nome=nome)
    await state.set_state(ReceitaStates.aguardando_valor)
    await message.answer(
        f"📌 *{md(message.text.strip())}*\n\nQual foi o valor em reais\\?\n_Ex: 3500 ou 800,50_",
        parse_mode="MarkdownV2")


@router.message(ReceitaStates.aguardando_valor)
async def receita_valor(message: Message, state: FSMContext):
    try:
        valor = parse_valor(message.text)
        if valor <= 0:
            raise ValueError
    except ValueError:
        await message.answer(f"❌ Valor inválido: `{md(message.text.strip())}`", parse_mode="MarkdownV2")
        return
    data = await state.get_data()
    user_id = message.from_user.id
    registrar_receita(user_id, data["nome"], valor)
    hoje = datetime.now().date()
    inicio_mes = hoje.replace(day=1)
    receitas_mes = get_receitas_periodo(user_id, inicio_mes.isoformat(), hoje.isoformat())
    total_mes_r = sum(r["valor"] for r in receitas_mes)
    meta_txt = alerta_meta_receita(user_id, total_mes_r)
    await state.clear()
    await message.answer(
        f"✅ *Receita registrada\\!*\n\n📌 {md(data['nome'])}\n💰 {md(formatar_valor(valor))}{meta_txt}",
        parse_mode="MarkdownV2", reply_markup=build_keyboard(user_id))


# ─── /investimento ────────────────────────────────────────────────────────────

@router.message(Command("investimento"))
async def cmd_investimento(message: Message, state: FSMContext):
    if is_rate_limited(message.from_user.id):
        await message.answer("⚠️ Devagar\\! Muitas mensagens em pouco tempo\\.", parse_mode="MarkdownV2")
        asyncio.create_task(notificar_rate_limit(message.from_user))
        return
    await state.clear()
    registrar_uso_comando(message.from_user.id, "/investimento")
    await state.set_state(InvestimentoStates.aguardando_nome)
    await message.answer(
        "📈 *Registrar Investimento*\n\nQual é o nome do investimento\\?\n"
        "_Ex: Tesouro Direto, CDB Inter, PETR4_\n\n_/cancelar para sair_",
        parse_mode="MarkdownV2")


@router.message(InvestimentoStates.aguardando_nome)
async def investimento_nome(message: Message, state: FSMContext):
    nome = message.text.strip()
    if len(nome) > 100:
        await message.answer("❌ Nome muito longo\\. Use até 100 caracteres\\.", parse_mode="MarkdownV2")
        return
    await state.update_data(nome=nome)
    await state.set_state(InvestimentoStates.aguardando_valor)
    await message.answer(
        f"📌 *{md(message.text.strip())}*\n\nQual foi o valor investido\\?\n_Ex: 500 ou 1200,50_",
        parse_mode="MarkdownV2")


@router.message(InvestimentoStates.aguardando_valor)
async def investimento_valor(message: Message, state: FSMContext):
    try:
        valor = parse_valor(message.text)
        if valor <= 0:
            raise ValueError
    except ValueError:
        await message.answer(f"❌ Valor inválido: `{md(message.text.strip())}`", parse_mode="MarkdownV2")
        return
    data = await state.get_data()
    user_id = message.from_user.id
    registrar_investimento(user_id, data["nome"], valor)
    hoje = datetime.now().date()
    inicio_mes = hoje.replace(day=1)
    inv_mes = get_investimentos_periodo(user_id, inicio_mes.isoformat(), hoje.isoformat())
    total_mes_i = sum(i["valor"] for i in inv_mes)
    meta_txt = alerta_meta_investimento(user_id, total_mes_i)
    await state.clear()
    await message.answer(
        f"✅ *Investimento registrado\\!*\n\n📌 {md(data['nome'])}\n📈 {md(formatar_valor(valor))}{meta_txt}",
        parse_mode="MarkdownV2", reply_markup=build_keyboard(user_id))


# ─── /meus_investimentos ──────────────────────────────────────────────────────

@router.message(Command("meus_investimentos"))
async def cmd_meus_investimentos(message: Message, state: FSMContext):
    if is_rate_limited(message.from_user.id):
        await message.answer("⚠️ Devagar\\! Muitas mensagens em pouco tempo\\.", parse_mode="MarkdownV2")
        asyncio.create_task(notificar_rate_limit(message.from_user))
        return
    await state.clear()
    user_id = message.from_user.id
    registrar_uso_comando(user_id, "/meus_investimentos")
    dados = get_investimentos(user_id)
    if not dados:
        await message.answer("📊 Nenhum investimento registrado ainda\\.", parse_mode="MarkdownV2")
        return
    total = sum(row["valor"] for row in dados)
    msg = f"📈 *Meus Investimentos*\nTotal: *{md(formatar_valor(total))}*\n\n"
    for row in dados:
        msg += f"🔹 \\#{row['id']} *{md(row['nome'])}* — {md(formatar_valor(row['valor']))}\n"
    msg += "\n_/editar\\_investimento | /deletar\\_investimento_"
    await message.answer(msg, parse_mode="MarkdownV2", reply_markup=build_keyboard(user_id))


# ─── /editar_investimento ─────────────────────────────────────────────────────

@router.message(Command("editar_investimento"))
async def cmd_editar_investimento(message: Message, state: FSMContext):
    if is_rate_limited(message.from_user.id):
        await message.answer("⚠️ Devagar\\! Muitas mensagens em pouco tempo\\.", parse_mode="MarkdownV2")
        asyncio.create_task(notificar_rate_limit(message.from_user))
        return
    await state.clear()
    user_id = message.from_user.id
    dados = get_investimentos(user_id)
    if not dados:
        await message.answer("📊 Nenhum investimento registrado ainda\\.", parse_mode="MarkdownV2")
        return
    msg = "✏️ *Editar Investimento*\n\n"
    for row in dados:
        msg += f"🔹 \\#{row['id']} *{md(row['nome'])}* — {md(formatar_valor(row['valor']))}\n"
    msg += "\nQual o *ID* do investimento que deseja editar?\n\n_/cancelar para sair_"
    await state.set_state(EditarInvStates.aguardando_id)
    await message.answer(msg, parse_mode="MarkdownV2")


@router.message(EditarInvStates.aguardando_id)
async def editar_inv_id(message: Message, state: FSMContext):
    try:
        inv_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ ID inválido\\.", parse_mode="MarkdownV2")
        return
    inv = get_investimento_by_id(message.from_user.id, inv_id)
    if not inv:
        await message.answer(f"❌ ID \\#{inv_id} não encontrado\\.", parse_mode="MarkdownV2")
        return
    await state.update_data(inv_id=inv_id, nome_atual=inv["nome"], valor_atual=inv["valor"])
    await state.set_state(EditarInvStates.aguardando_nome)
    await message.answer(
        f"✏️ Editando: *{md(inv['nome'])}* — {md(formatar_valor(inv['valor']))}\n\n"
        "Novo nome\\? _\\(ou \\. para manter\\)_", parse_mode="MarkdownV2")


@router.message(EditarInvStates.aguardando_nome)
async def editar_inv_nome(message: Message, state: FSMContext):
    data = await state.get_data()
    novo_nome = data["nome_atual"] if message.text.strip() == "." else message.text.strip()
    await state.update_data(novo_nome=novo_nome)
    await state.set_state(EditarInvStates.aguardando_valor)
    await message.answer(
        f"Nome: *{md(novo_nome)}*\n\nNovo valor\\? _\\(ou \\. para manter\\)_",
        parse_mode="MarkdownV2")


@router.message(EditarInvStates.aguardando_valor)
async def editar_inv_valor(message: Message, state: FSMContext):
    data = await state.get_data()
    txt = message.text.strip()
    if txt == ".":
        novo_valor = data["valor_atual"]
    else:
        try:
            novo_valor = parse_valor(txt)
            if novo_valor <= 0:
                raise ValueError
        except ValueError:
            await message.answer(f"❌ Valor inválido: `{md(txt)}`", parse_mode="MarkdownV2")
            return
    editar_investimento(message.from_user.id, data["inv_id"], data["novo_nome"], novo_valor)
    await state.clear()
    await message.answer(
        f"✅ *Investimento atualizado\\!*\n\n📌 {md(data['novo_nome'])}\n📈 {md(formatar_valor(novo_valor))}",
        parse_mode="MarkdownV2", reply_markup=build_keyboard(message.from_user.id))


# ─── /deletar_investimento ────────────────────────────────────────────────────

@router.message(Command("deletar_investimento"))
async def cmd_deletar_investimento(message: Message, state: FSMContext):
    if is_rate_limited(message.from_user.id):
        await message.answer("⚠️ Devagar\\! Muitas mensagens em pouco tempo\\.", parse_mode="MarkdownV2")
        asyncio.create_task(notificar_rate_limit(message.from_user))
        return
    await state.clear()
    user_id = message.from_user.id
    dados = get_investimentos(user_id)
    if not dados:
        await message.answer("📊 Nenhum investimento registrado ainda\\.", parse_mode="MarkdownV2")
        return
    msg = "🗑️ *Deletar Investimento*\n\n"
    for row in dados:
        msg += f"🔹 \\#{row['id']} *{md(row['nome'])}* — {md(formatar_valor(row['valor']))}\n"
    msg += "\nQual o *ID* que deseja excluir?\n\n_/cancelar para sair_"
    await state.set_state(DeletarInvStates.aguardando_id)
    await message.answer(msg, parse_mode="MarkdownV2")


@router.message(DeletarInvStates.aguardando_id)
async def deletar_inv_id(message: Message, state: FSMContext):
    try:
        inv_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ ID inválido\\.", parse_mode="MarkdownV2")
        return
    user_id = message.from_user.id
    inv = get_investimento_by_id(user_id, inv_id)
    if not inv:
        await message.answer(f"❌ ID \\#{inv_id} não encontrado\\.", parse_mode="MarkdownV2")
        return
    await state.update_data(inv_id=inv_id)
    await state.set_state(DeletarInvStates.aguardando_confirmacao)
    await message.answer(
        f"⚠️ *Confirmar exclusão?*\n\n📌 {md(inv['nome'])} — {md(formatar_valor(inv['valor']))}\n\n"
        "Digite *sim* para confirmar ou /cancelar:",
        parse_mode="MarkdownV2")


@router.message(DeletarInvStates.aguardando_confirmacao)
async def deletar_inv_confirmar(message: Message, state: FSMContext):
    if message.text.strip().lower() != "sim":
        await message.answer("Operação cancelada\\.", parse_mode="MarkdownV2",
                             reply_markup=build_keyboard(message.from_user.id))
        await state.clear()
        return
    data = await state.get_data()
    user_id = message.from_user.id
    sucesso = deletar_investimento(user_id, data["inv_id"])
    await state.clear()
    if sucesso:
        await message.answer(f"🗑️ Investimento \\#{data['inv_id']} excluído\\!",
                             parse_mode="MarkdownV2", reply_markup=build_keyboard(user_id))
    else:
        await message.answer("❌ Não foi possível excluir\\.", parse_mode="MarkdownV2")


# ─── /receber ─────────────────────────────────────────────────────────────────

@router.message(Command("receber"))
async def cmd_receber(message: Message, state: FSMContext):
    if is_rate_limited(message.from_user.id):
        await message.answer("⚠️ Devagar\\! Muitas mensagens em pouco tempo\\.", parse_mode="MarkdownV2")
        asyncio.create_task(notificar_rate_limit(message.from_user))
        return
    await state.clear()
    registrar_uso_comando(message.from_user.id, "/receber")
    await state.set_state(ReceberStates.aguardando_nome)
    await message.answer(
        "💵 *Registrar A Receber*\n\nQual a descrição do valor a receber\\?\n"
        "_Ex: Freela cliente X, Aluguel_\n\n_/cancelar para sair_",
        parse_mode="MarkdownV2")


@router.message(ReceberStates.aguardando_nome)
async def receber_nome(message: Message, state: FSMContext):
    nome = message.text.strip()
    if len(nome) > 100:
        await message.answer("❌ Nome muito longo\\. Use até 100 caracteres\\.", parse_mode="MarkdownV2")
        return
    await state.update_data(nome=nome)
    await state.set_state(ReceberStates.aguardando_valor)
    await message.answer(
        f"📌 *{md(message.text.strip())}*\n\nQual o valor em reais\\?\n_Ex: 600 ou 900,50_",
        parse_mode="MarkdownV2")


@router.message(ReceberStates.aguardando_valor)
async def receber_valor(message: Message, state: FSMContext):
    try:
        valor = parse_valor(message.text)
        if valor <= 0:
            raise ValueError
    except ValueError:
        await message.answer(f"❌ Valor inválido: `{md(message.text.strip())}`", parse_mode="MarkdownV2")
        return
    await state.update_data(valor=valor)
    await state.set_state(ReceberStates.aguardando_data)
    await message.answer("📅 Para qual data está previsto?\n_Formato: dd/mm — Ex: 15/06_",
                         parse_mode="MarkdownV2")


@router.message(ReceberStates.aguardando_data)
async def receber_data(message: Message, state: FSMContext):
    data_str = message.text.strip()
    match = re.fullmatch(r"(\d{1,2})/(\d{1,2})", data_str)
    if not match or not (1 <= int(match.group(1)) <= 31 and 1 <= int(match.group(2)) <= 12):
        await message.answer(f"❌ Data inválida: `{md(data_str)}`\n\nUse `dd/mm`, ex: `15/06`",
                             parse_mode="MarkdownV2")
        return
    data = await state.get_data()
    registrar_recebimento(message.from_user.id, data["nome"], data["valor"], data_str)
    await state.clear()
    await message.answer(
        f"✅ *A receber registrado\\!*\n\n📌 {md(data['nome'])}\n💵 {md(formatar_valor(data['valor']))}\n📅 Previsto para {md(data_str)}",
        parse_mode="MarkdownV2", reply_markup=build_keyboard(message.from_user.id))


# ─── /resumo ──────────────────────────────────────────────────────────────────

@router.message(Command("resumo"))
async def cmd_resumo(message: Message, state: FSMContext):
    if is_rate_limited(message.from_user.id):
        await message.answer("⚠️ Devagar\\! Muitas mensagens em pouco tempo\\.", parse_mode="MarkdownV2")
        asyncio.create_task(notificar_rate_limit(message.from_user))
        return
    await state.clear()
    user_id = message.from_user.id
    body = get_body(message.text).lower().strip().replace("ê", "e")

    if body not in ("semana", "mes", "ano"):
        await message.answer("❌ Use `/resumo semana`, `/resumo mes` ou `/resumo ano`",
                             parse_mode="MarkdownV2")
        return

    registrar_uso_comando(user_id, f"/resumo {body}")
    hoje = datetime.now().date()

    if body == "semana":
        data_inicio = hoje - timedelta(days=6)
        data_inicio_ant = data_inicio - timedelta(days=7)
        data_fim_ant = data_inicio - timedelta(days=1)
        titulo = "📅 Resumo — Últimos 7 dias"
    elif body == "mes":
        data_inicio = hoje - timedelta(days=29)
        data_inicio_ant = data_inicio - timedelta(days=30)
        data_fim_ant = data_inicio - timedelta(days=1)
        titulo = "📅 Resumo — Últimos 30 dias"
    else:
        data_inicio = hoje.replace(month=1, day=1)
        data_inicio_ant = data_inicio.replace(year=data_inicio.year - 1)
        data_fim_ant = data_inicio - timedelta(days=1)
        titulo = f"📅 Resumo — {hoje.year}"

    gastos = get_gastos_periodo(user_id, data_inicio.isoformat(), hoje.isoformat())
    receitas = get_receitas_periodo(user_id, data_inicio.isoformat(), hoje.isoformat())
    investimentos = get_investimentos_periodo(user_id, data_inicio.isoformat(), hoje.isoformat())
    gastos_ant = get_gastos_periodo(user_id, data_inicio_ant.isoformat(), data_fim_ant.isoformat())

    total_gastos = sum(g["valor"] for g in gastos)
    total_receitas = sum(r["valor"] for r in receitas)
    total_inv = sum(i["valor"] for i in investimentos)
    total_gastos_ant = sum(g["valor"] for g in gastos_ant)
    saldo = total_receitas - total_gastos - total_inv
    saldo_emoji = "🟢" if saldo >= 0 else "🔴"

    if total_gastos_ant > 0:
        variacao = ((total_gastos - total_gastos_ant) / total_gastos_ant) * 100
        sinal = "📈 \\+" if variacao > 0 else "📉 "
        comp = f"\n{sinal}{md(f'{variacao:.1f}')}% em gastos vs período anterior"
    else:
        comp = ""

    periodo_str = f"{data_inicio.strftime('%d/%m')} a {hoje.strftime('%d/%m/%Y')}"
    msg = (
        f"*{md(titulo)}*\n_{md(periodo_str)}_\n\n"
        f"💰 Receitas: *{md(formatar_valor(total_receitas))}* \\({len(receitas)}x\\)\n"
        f"💸 Gastos: *{md(formatar_valor(total_gastos))}* \\({len(gastos)}x\\)\n"
        f"📈 Investimentos: *{md(formatar_valor(total_inv))}* \\({len(investimentos)}x\\)\n"
        f"{comp}\n"
        f"{saldo_emoji} Saldo: *{md(formatar_valor(saldo))}*\n"
    )

    if body == "mes":
        msg += alerta_meta(user_id, total_gastos)
        meta_id = get_meta_inv_data(user_id)
        if meta_id:
            inicio_ano = hoje.replace(month=1, day=1)
            inv_ano = get_investimentos_periodo(user_id, inicio_ano.isoformat(), hoje.isoformat())
            total_inv_acum = sum(i["valor"] for i in inv_ano)
            msg += alerta_meta_investimento(user_id, total_inv_acum)
        else:
            msg += alerta_meta_investimento(user_id, total_inv)
        msg += alerta_meta_receita(user_id, total_receitas)

    if gastos:
        msg += "\n📋 *Gastos do período:*\n"
        for g in gastos[:10]:
            estrelas = ESTRELAS[g["utilidade"] - 1] if g["utilidade"] > 0 else "☆"
            msg += f"  • {md(g['nome'])} — {md(formatar_valor(g['valor']))} {estrelas}\n"
        if len(gastos) > 10:
            msg += f"  _\\+{len(gastos) - 10} mais\\._\n"

    await message.answer(msg, parse_mode="MarkdownV2", reply_markup=build_keyboard(user_id))


# ─── /rank ────────────────────────────────────────────────────────────────────

@router.message(Command("rank"))
async def cmd_rank(message: Message, state: FSMContext):
    if is_rate_limited(message.from_user.id):
        await message.answer("⚠️ Devagar\\! Muitas mensagens em pouco tempo\\.", parse_mode="MarkdownV2")
        asyncio.create_task(notificar_rate_limit(message.from_user))
        return
    await state.clear()
    user_id = message.from_user.id
    registrar_uso_comando(user_id, "/rank")
    dados = get_rank_gastos(user_id)
    if not dados:
        await message.answer("📊 Nenhum gasto registrado ainda\\.", parse_mode="MarkdownV2")
        return
    msg = "🏆 *Ranking de Gastos*\n\n"
    for i, row in enumerate(dados, 1):
        medalha = ["🥇", "🥈", "🥉"][i - 1] if i <= 3 else f"{i}\\."
        msg += f"{medalha} *{md(row['nome'])}*\n   {md(formatar_valor(row['total']))} — {row['qtd']} vez\\(es\\)\n"
    await message.answer(msg, parse_mode="MarkdownV2", reply_markup=build_keyboard(user_id))


# ─── /utilidade ───────────────────────────────────────────────────────────────

@router.message(Command("utilidade"))
async def cmd_utilidade(message: Message, state: FSMContext):
    if is_rate_limited(message.from_user.id):
        await message.answer("⚠️ Devagar\\! Muitas mensagens em pouco tempo\\.", parse_mode="MarkdownV2")
        asyncio.create_task(notificar_rate_limit(message.from_user))
        return
    await state.clear()
    user_id = message.from_user.id
    registrar_uso_comando(user_id, "/utilidade")
    dados = get_media_utilidade(user_id)
    media_geral = get_media_utilidade_geral(user_id)
    if not dados:
        await message.answer("📊 Nenhum gasto registrado ainda\\.", parse_mode="MarkdownV2")
        return
    msg = (
        "🎯 *Análise de Utilidade*\n\n"
        f"Média geral: *{md(f'{media_geral:.1f}')}/5*\n"
        "_Do menos ao mais útil:_\n\n"
    )
    for row in dados:
        media = row["media"]
        estrelas = ESTRELAS[round(media) - 1] if round(media) > 0 else "☆"
        msg += f"• *{md(row['nome'])}*\n  {md(f'{media:.1f}')}/5 {estrelas} — {md(formatar_valor(row['total']))}\n"
    await message.answer(msg, parse_mode="MarkdownV2", reply_markup=build_keyboard(user_id))


# ─── /pendentes ───────────────────────────────────────────────────────────────

@router.message(Command("pendentes"))
async def cmd_pendentes(message: Message, state: FSMContext):
    if is_rate_limited(message.from_user.id):
        await message.answer("⚠️ Devagar\\! Muitas mensagens em pouco tempo\\.", parse_mode="MarkdownV2")
        asyncio.create_task(notificar_rate_limit(message.from_user))
        return
    await state.clear()
    user_id = message.from_user.id
    registrar_uso_comando(user_id, "/pendentes")
    dados = get_pendentes(user_id)
    if not dados:
        await message.answer("✅ Nenhum valor pendente a receber\\!", parse_mode="MarkdownV2")
        return
    total = sum(r["valor"] for r in dados)
    msg = f"💵 *Valores a Receber*\nTotal: *{md(formatar_valor(total))}*\n\n"
    for row in dados:
        vencido = ""
        try:
            dia, mes = map(int, row["data_prevista"].split("/"))
            ano = datetime.now().year if mes >= datetime.now().month else datetime.now().year + 1
            if datetime(ano, mes, dia).date() < datetime.now().date():
                vencido = " ⚠️ VENCIDO"
        except Exception:
            pass
        msg += (
            f"🔹 *{md(row['nome'])}*{md(vencido)}\n"
            f"   {md(formatar_valor(row['valor']))} — 📅 {md(row['data_prevista'])}\n"
            f"   _/recebido {row['id']}_\n\n"
        )
    await message.answer(msg, parse_mode="MarkdownV2", reply_markup=build_keyboard(user_id))


# ─── /recebido ────────────────────────────────────────────────────────────────

@router.message(Command("recebido"))
async def cmd_recebido(message: Message, state: FSMContext):
    if is_rate_limited(message.from_user.id):
        await message.answer("⚠️ Devagar\\! Muitas mensagens em pouco tempo\\.", parse_mode="MarkdownV2")
        asyncio.create_task(notificar_rate_limit(message.from_user))
        return
    await state.clear()
    user_id = message.from_user.id
    body = get_body(message.text).strip()
    if not body:
        await message.answer("❌ Informe o ID\\. Ex: `/recebido 3`\n_Veja os IDs em /pendentes_",
                             parse_mode="MarkdownV2")
        return
    try:
        rec_id = int(body.split()[0])
    except ValueError:
        await message.answer(f"❌ ID inválido: `{md(body)}`", parse_mode="MarkdownV2")
        return
    sucesso = marcar_recebido(user_id, rec_id)
    if sucesso:
        await message.answer(f"✅ Recebimento \\#{rec_id} marcado como recebido\\!",
                             parse_mode="MarkdownV2", reply_markup=build_keyboard(user_id))
    else:
        await message.answer(f"❌ ID \\#{rec_id} não encontrado\\. Veja /pendentes\\.",
                             parse_mode="MarkdownV2")


# ─── main ─────────────────────────────────────────────────────────────────────

async def main():
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise ValueError("TELEGRAM_BOT_TOKEN não definido.")

    if not ADMIN_ID:
        logger.warning("ADMIN_ID não definido. Notificações de admin desativadas.")

    logging.getLogger().addFilter(TokenFilter(token))

    init_db()

    global _bot_ref
    bot = Bot(token=token)
    _bot_ref = bot
    storage = SqliteStorage("fsm_storage.db")
    dp = Dispatcher(storage=storage)
    dp.include_router(router)

    await bot.set_my_commands([
        BotCommand(command="gasto",                description="💸 Registrar um gasto"),
        BotCommand(command="receita",              description="💰 Registrar uma receita"),
        BotCommand(command="investimento",         description="📈 Registrar um investimento"),
        BotCommand(command="receber",              description="💵 Registrar valor a receber"),
        BotCommand(command="meta",                 description="🎯 Definir metas mensais"),
        BotCommand(command="resumo",               description="📊 Resumo (semana / mes / ano)"),
        BotCommand(command="pendentes",            description="🔔 Valores a receber em aberto"),
        BotCommand(command="rank",                 description="🏆 Ranking de gastos"),
        BotCommand(command="utilidade",            description="⭐ Análise de utilidade"),
        BotCommand(command="deletar_gasto",        description="🗑️ Excluir um gasto"),
        BotCommand(command="meus_investimentos",   description="📋 Listar investimentos"),
        BotCommand(command="editar_investimento",  description="✏️ Editar investimento"),
        BotCommand(command="deletar_investimento", description="🗑️ Excluir investimento"),
        BotCommand(command="ajuda",                description="📖 Ver todos os comandos"),
        BotCommand(command="cancelar",             description="❌ Cancelar operação atual"),
    ])

    async def handle_index(request):
        response = web.FileResponse("index.html")
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
            "font-src https://fonts.gstatic.com; "
            "img-src 'self' https://i.imgur.com; "
            "script-src 'self' 'unsafe-inline';"
        )
        return response

    async def handle_logo(request):
        return web.FileResponse("logo.jpg")

    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/logo.jpg", handle_logo)

    port = int(os.environ.get("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Site rodando na porta {port}.")

    logger.info("Minerva Finance iniciado.")
    asyncio.create_task(agendar_relatorio_diario())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
