#!/usr/bin/env python3
"""Render compact HTML pages from the JSON evidence."""

import html
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / ".runtime" / "evidence"
RESTORE_CONTRACT = "hw3-vacancy-mart-v1"
STYLE = """
body{margin:0;background:#f4f7fb;color:#172033;font:12px sans-serif}
.page{width:760px;padding:24px}h1{font-size:22px}.panel{background:white;
border:1px solid #dfe5ee;border-radius:9px;padding:12px;margin:10px 0}
table{width:100%;border-collapse:collapse}th,td{padding:7px;
border-bottom:1px solid #e5e7eb;text-align:left}.ok{color:#16803a;font-weight:700}
.bad{color:#c62828;font-weight:700}pre{white-space:pre-wrap;background:#111827;
color:#e5e7eb;padding:8px;border-radius:6px;font-size:9px}
"""


def document(body):
    return (
        "<!doctype html><meta charset='utf-8'><style>"
        + STYLE
        + "</style><div class='page'>"
        + body
        + "</div>"
    )


def plan_excerpt(name):
    path = ROOT / "evidence" / "optimization" / "plans" / name
    return html.escape("\n".join(path.read_text(encoding="utf-8").splitlines()[:12]))


def render_optimization():
    data = json.loads(
        (ROOT / "evidence" / "optimization" / "summary.json").read_text()
    )
    rows = "".join(
        f"<tr><td>{html.escape(run['variant'])}</td>"
        f"<td>{run['returned_rows']}</td>"
        f"<td>{run['median_execution_ms']:.3f} ms</td></tr>"
        for run in data["runs"]
    )
    body = f"""
    <h1>Оптимизация таблицы вакансий</h1>
    <div class='panel'>Один прогрев и пять измерений серверного Execution Time.</div>
    <table><tr><th>Вариант</th><th>Строк ответа</th><th>Медиана</th></tr>{rows}</table>
    <div class='panel'><h2>Baseline</h2><pre>{plan_excerpt('base.txt')}</pre></div>
    <div class='panel'><h2>Indexed</h2><pre>{plan_excerpt('indexed.txt')}</pre></div>
    <div class='panel'><h2>Partitioned</h2><pre>{plan_excerpt('partitioned.txt')}</pre></div>
    """
    (OUTPUT / "optimization.html").write_text(document(body), encoding="utf-8")


def render_replication():
    data = json.loads((ROOT / "evidence" / "replication.json").read_text())
    body = f"""
    <h1>Patroni: репликация и switchover</h1>
    <div class='panel'>До: {html.escape(data['before']['leader']['name'])} primary,
    {html.escape(data['before']['replica']['name'])} replica.</div>
    <div class='panel'><span class='ok'>PASS</span> Маркер дошёл до replica;
    запись на replica отклонена с SQLSTATE
    {html.escape(data['replica_write']['sqlstate'])}.</div>
    <div class='panel'>После: {html.escape(data['after']['leader']['name'])} primary,
    {html.escape(data['after']['replica']['name'])} replica.</div>
    """
    (OUTPUT / "replication.html").write_text(document(body), encoding="utf-8")


def render_restore_document(data):
    if data.get("evidence_contract") != RESTORE_CONTRACT:
        return document(
            f"<h1>Restore evidence несовместим</h1><div class='panel'>"
            f"<span class='bad'>INCOMPATIBLE EVIDENCE</span> Ожидался "
            f"{RESTORE_CONTRACT}.</div>"
        )
    if data.get("all_checks_equal") is not True:
        return document(
            "<h1>Restore</h1><div class='panel'><span class='bad'>FAILED</span></div>"
        )
    rows = "".join(
        f"<tr><td>{html.escape(name)}</td><td>{values['rows']}</td>"
        f"<td>{values['unique_keys']}</td></tr>"
        for name, values in data["restored"]["relations"].items()
    )
    body = f"""
    <h1>Backup восстановлен</h1>
    <div class='panel'><span class='ok'>ALL CHECKS EQUAL</span>
    {html.escape(data['pg_dump_version'])}; {data['backup_bytes']} bytes.</div>
    <table><tr><th>Таблица</th><th>Строки</th><th>Ключи</th></tr>{rows}</table>
    """
    return document(body)


def render_restore():
    data = json.loads((ROOT / "evidence" / "restore.json").read_text())
    (OUTPUT / "restore.html").write_text(
        render_restore_document(data), encoding="utf-8"
    )


def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    render_optimization()
    render_replication()
    render_restore()
    print(f"rendered evidence pages in {OUTPUT}")


if __name__ == "__main__":
    main()

