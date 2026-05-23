import sqlite3
from datetime import datetime

DB_NAME = "minerva_finance.db"


def get_connection():
    conn = sqlite3.connect(DB_NAME, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")    # FIX #6 — Write-Ahead Logging evita database locked e corrupção
    conn.execute("PRAGMA synchronous=NORMAL")  # Mais rápido, ainda seguro
    conn.execute("PRAGMA foreign_keys=ON")     # Garante integridade referencial
    return conn


def init_db():
    conn = get_connection()
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS usuarios (
        user_id INTEGER PRIMARY KEY,
        primeiro_acesso TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS gastos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        nome TEXT NOT NULL,
        valor REAL NOT NULL,
        utilidade INTEGER NOT NULL CHECK(utilidade >= 0 AND utilidade <= 5),
        data_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS receitas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        nome TEXT NOT NULL,
        valor REAL NOT NULL,
        data_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS investimentos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        nome TEXT NOT NULL,
        valor REAL NOT NULL,
        data_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS recebimentos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        nome TEXT NOT NULL,
        valor REAL NOT NULL,
        data_prevista TEXT NOT NULL,
        recebido INTEGER DEFAULT 0,
        data_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS command_usage (
        user_id INTEGER NOT NULL,
        comando TEXT NOT NULL,
        total INTEGER DEFAULT 1,
        PRIMARY KEY (user_id, comando)
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS metas (
        user_id INTEGER PRIMARY KEY,
        valor REAL NOT NULL
    )""")
    # Adiciona colunas de meta de investimentos e receita (migração suave)
    try:
        c.execute("ALTER TABLE metas ADD COLUMN meta_investimento REAL")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE metas ADD COLUMN meta_receita REAL")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE metas ADD COLUMN meta_inv_data REAL")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE metas ADD COLUMN meta_inv_prazo TEXT")
    except Exception:
        pass
    conn.commit()
    conn.close()


def is_first_visit(user_id: int) -> bool:
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT user_id FROM usuarios WHERE user_id = ?", (user_id,))
    existe = c.fetchone()
    if not existe:
        c.execute("INSERT INTO usuarios (user_id) VALUES (?)", (user_id,))
        conn.commit()
        conn.close()
        return True
    conn.close()
    return False


def registrar_gasto(user_id, nome, valor, utilidade):
    conn = get_connection()
    c = conn.cursor()
    c.execute("INSERT INTO gastos (user_id, nome, valor, utilidade) VALUES (?, ?, ?, ?)",
              (user_id, nome, valor, utilidade))
    conn.commit()
    conn.close()


def deletar_gasto(user_id, gasto_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM gastos WHERE id = ? AND user_id = ?", (gasto_id, user_id))
    row = c.fetchone()
    if row:
        c.execute("DELETE FROM gastos WHERE id = ? AND user_id = ?", (gasto_id, user_id))
        conn.commit()
    conn.close()
    return row


def get_gastos_recentes(user_id, limit=10):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM gastos WHERE user_id = ? ORDER BY data_registro DESC LIMIT ?",
              (user_id, limit))
    rows = c.fetchall()
    conn.close()
    return rows


def registrar_receita(user_id, nome, valor):
    conn = get_connection()
    c = conn.cursor()
    c.execute("INSERT INTO receitas (user_id, nome, valor) VALUES (?, ?, ?)", (user_id, nome, valor))
    conn.commit()
    conn.close()


def registrar_investimento(user_id, nome, valor):
    conn = get_connection()
    c = conn.cursor()
    c.execute("INSERT INTO investimentos (user_id, nome, valor) VALUES (?, ?, ?)", (user_id, nome, valor))
    conn.commit()
    conn.close()


def registrar_recebimento(user_id, nome, valor, data_prevista):
    conn = get_connection()
    c = conn.cursor()
    c.execute("INSERT INTO recebimentos (user_id, nome, valor, data_prevista) VALUES (?, ?, ?, ?)",
              (user_id, nome, valor, data_prevista))
    conn.commit()
    conn.close()


def get_gastos_periodo(user_id, data_inicio, data_fim):
    conn = get_connection()
    c = conn.cursor()
    c.execute("""SELECT * FROM gastos WHERE user_id = ?
               AND date(data_registro) >= date(?) AND date(data_registro) <= date(?)
               ORDER BY data_registro DESC""", (user_id, data_inicio, data_fim))
    rows = c.fetchall()
    conn.close()
    return rows


def get_receitas_periodo(user_id, data_inicio, data_fim):
    conn = get_connection()
    c = conn.cursor()
    c.execute("""SELECT * FROM receitas WHERE user_id = ?
               AND date(data_registro) >= date(?) AND date(data_registro) <= date(?)
               ORDER BY data_registro DESC""", (user_id, data_inicio, data_fim))
    rows = c.fetchall()
    conn.close()
    return rows


def get_investimentos_periodo(user_id, data_inicio, data_fim):
    conn = get_connection()
    c = conn.cursor()
    c.execute("""SELECT * FROM investimentos WHERE user_id = ?
               AND date(data_registro) >= date(?) AND date(data_registro) <= date(?)
               ORDER BY data_registro DESC""", (user_id, data_inicio, data_fim))
    rows = c.fetchall()
    conn.close()
    return rows


def get_rank_gastos(user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("""SELECT nome, SUM(valor) as total, COUNT(*) as qtd FROM gastos
               WHERE user_id = ? GROUP BY nome ORDER BY total DESC LIMIT 20""", (user_id,))
    rows = c.fetchall()
    conn.close()
    return rows


def get_media_utilidade(user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("""SELECT nome, AVG(utilidade) as media, COUNT(*) as qtd, SUM(valor) as total
               FROM gastos WHERE user_id = ? GROUP BY nome ORDER BY media ASC""", (user_id,))
    rows = c.fetchall()
    conn.close()
    return rows


def get_media_utilidade_geral(user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT AVG(utilidade) as media FROM gastos WHERE user_id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row["media"] if row and row["media"] is not None else 0


def get_pendentes(user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("""SELECT * FROM recebimentos WHERE user_id = ? AND recebido = 0
               ORDER BY data_prevista ASC""", (user_id,))
    rows = c.fetchall()
    conn.close()
    return rows


def marcar_recebido(user_id, recebimento_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("UPDATE recebimentos SET recebido = 1 WHERE id = ? AND user_id = ?",
              (recebimento_id, user_id))
    affected = c.rowcount
    conn.commit()
    conn.close()
    return affected > 0


def get_investimentos(user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM investimentos WHERE user_id = ? ORDER BY data_registro DESC", (user_id,))
    rows = c.fetchall()
    conn.close()
    return rows


def deletar_investimento(user_id, inv_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("DELETE FROM investimentos WHERE id = ? AND user_id = ?", (inv_id, user_id))
    affected = c.rowcount
    conn.commit()
    conn.close()
    return affected > 0


def editar_investimento(user_id, inv_id, novo_nome, novo_valor):
    conn = get_connection()
    c = conn.cursor()
    c.execute("UPDATE investimentos SET nome = ?, valor = ? WHERE id = ? AND user_id = ?",
              (novo_nome, novo_valor, inv_id, user_id))
    affected = c.rowcount
    conn.commit()
    conn.close()
    return affected > 0


def get_investimento_by_id(user_id, inv_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM investimentos WHERE id = ? AND user_id = ?", (inv_id, user_id))
    row = c.fetchone()
    conn.close()
    return row


def registrar_uso_comando(user_id: int, comando: str):
    conn = get_connection()
    c = conn.cursor()
    c.execute("""INSERT INTO command_usage (user_id, comando, total) VALUES (?, ?, 1)
               ON CONFLICT(user_id, comando) DO UPDATE SET total = total + 1""",
              (user_id, comando))
    conn.commit()
    conn.close()


def get_comandos_mais_usados(user_id: int, limit: int = 6) -> list:
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT comando FROM command_usage WHERE user_id = ? ORDER BY total DESC LIMIT ?",
              (user_id, limit))
    rows = c.fetchall()
    conn.close()
    return [row["comando"] for row in rows]


def definir_meta(user_id: int, valor: float):
    conn = get_connection()
    c = conn.cursor()
    c.execute("INSERT INTO metas (user_id, valor) VALUES (?, ?) ON CONFLICT(user_id) DO UPDATE SET valor=excluded.valor",
              (user_id, valor))
    conn.commit()
    conn.close()


def get_meta(user_id: int):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT valor FROM metas WHERE user_id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row["valor"] if row else None


def definir_meta_investimento(user_id: int, valor: float):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        "INSERT INTO metas (user_id, valor, meta_investimento) VALUES (?, COALESCE((SELECT valor FROM metas WHERE user_id=?), 0), ?) "
        "ON CONFLICT(user_id) DO UPDATE SET meta_investimento=excluded.meta_investimento",
        (user_id, user_id, valor))
    conn.commit()
    conn.close()


def get_meta_investimento(user_id: int):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT meta_investimento FROM metas WHERE user_id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row["meta_investimento"] if row else None


def definir_meta_receita(user_id: int, valor: float):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        "INSERT INTO metas (user_id, valor, meta_receita) VALUES (?, COALESCE((SELECT valor FROM metas WHERE user_id=?), 0), ?) "
        "ON CONFLICT(user_id) DO UPDATE SET meta_receita=excluded.meta_receita",
        (user_id, user_id, valor))
    conn.commit()
    conn.close()


def get_meta_receita(user_id: int):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT meta_receita FROM metas WHERE user_id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row["meta_receita"] if row else None


def definir_meta_inv_data(user_id: int, valor: float, prazo: str):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        "INSERT INTO metas (user_id, valor, meta_inv_data, meta_inv_prazo) VALUES (?, COALESCE((SELECT valor FROM metas WHERE user_id=?), 0), ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET meta_inv_data=excluded.meta_inv_data, meta_inv_prazo=excluded.meta_inv_prazo",
        (user_id, user_id, valor, prazo))
    conn.commit()
    conn.close()


# FIX #2 — Retorna tuple (valor, prazo) em vez de dict, para compatibilidade com main.py
def get_meta_inv_data(user_id: int):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT meta_inv_data, meta_inv_prazo FROM metas WHERE user_id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    if row and row["meta_inv_data"] is not None:
        return (row["meta_inv_data"], row["meta_inv_prazo"])  # tuple, não dict
    return None

def get_stats_globais() -> dict:
    """Retorna estatísticas globais do bot para notificações ao admin."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) as total FROM usuarios")
    total_usuarios = c.fetchone()["total"]
    c.execute("SELECT COUNT(*) as total FROM gastos")
    total_gastos = c.fetchone()["total"]
    c.execute("SELECT COALESCE(SUM(valor),0) as total FROM gastos")
    volume_gastos = c.fetchone()["total"]
    c.execute("SELECT COUNT(*) as total FROM receitas")
    total_receitas = c.fetchone()["total"]
    c.execute("SELECT COUNT(*) as total FROM investimentos")
    total_investimentos = c.fetchone()["total"]
    c.execute("""SELECT COUNT(*) as total FROM gastos
                 WHERE date(data_registro) = date('now','localtime')""")
    gastos_hoje = c.fetchone()["total"]
    c.execute("""SELECT COUNT(DISTINCT user_id) as total FROM gastos
                 WHERE date(data_registro) = date('now','localtime')""")
    usuarios_ativos_hoje = c.fetchone()["total"]
    conn.close()
    return {
        "total_usuarios": total_usuarios,
        "total_gastos": total_gastos,
        "volume_gastos": volume_gastos,
        "total_receitas": total_receitas,
        "total_investimentos": total_investimentos,
        "gastos_hoje": gastos_hoje,
        "usuarios_ativos_hoje": usuarios_ativos_hoje,
    }
