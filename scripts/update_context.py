#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Simple updater for CONTEXT.md and sessions/*.md based on push event.
Designed to run in GitHub Actions (GITHUB_EVENT_PATH, GITHUB_REPOSITORY, GITHUB_TOKEN available).
"""
import json
import os
import re
import subprocess
from datetime import datetime, timezone

REPO = os.environ.get("GITHUB_REPOSITORY")
EVENT_PATH = os.environ.get("GITHUB_EVENT_PATH")
WORKDIR = os.environ.get("GITHUB_WORKSPACE", ".")

CONTEXT_PATH = os.path.join(WORKDIR, "CONTEXT.md")
SESSIONS_DIR = os.path.join(WORKDIR, "sessions")

os.makedirs(SESSIONS_DIR, exist_ok=True)

def load_push_files():
    if not EVENT_PATH or not os.path.exists(EVENT_PATH):
        return []
    with open(EVENT_PATH, "r", encoding="utf-8") as f:
        ev = json.load(f)
    files = []
    for c in ev.get("commits", []):
        files.extend(c.get("added", []) or [])
        files.extend(c.get("modified", []) or [])
    head = ev.get("head_commit") or {}
    files.extend(head.get("added", []) or [])
    files.extend(head.get("modified", []) or [])
    return sorted(set(files))

def ensure_context_exists():
    if not os.path.exists(CONTEXT_PATH):
        with open(CONTEXT_PATH, "w", encoding="utf-8") as f:
            f.write("# CONTEXT — краткая сводка проекта\n\n")
    return

def update_context_with_file(path, commit_sha=None):
    repo_url = f"https://github.com/{REPO}/blob/main/{path}"
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    with open(CONTEXT_PATH, "r", encoding="utf-8") as f:
        ctx = f.read()
    if repo_url in ctx:
        return False
    insert = f"- {os.path.basename(path)} — базовый шаблон (сохранён в репозитории): {repo_url}"
    if "Базовые шаблоны / важные файлы:" in ctx:
        parts = ctx.split("Базовые шаблоны / важные файлы:")
        left = parts[0] + "Базовые шаблоны / важные файлы:\n"
        right = parts[1]
        new = left + " " + insert + "\n" + right
        ctx = new
    else:
        ctx = ctx + "\nБазовые шаблоны / важные файлы:\n" + insert + "\n"
    ctx += f"\n- {now} — добавлен {os.path.basename(path)} (автообновление через Action)\n"
    with open(CONTEXT_PATH, "w", encoding="utf-8") as f:
        f.write(ctx)
    return True

def create_session_record(changed_files):
    now = datetime.now(timezone.utc)
    fname = now.strftime("sessions/%Y-%m-%d_%H%M.md")
    fullpath = os.path.join(WORKDIR, fname)
    lines = []
    lines.append(f"Дата: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append("Цель сессии: Автообновление контекста (GitHub Action)")
    lines.append("")
    lines.append("Изменённые/добавленные файлы:")
    for p in changed_files:
        lines.append(f"- {p}")
    lines.append("")
    lines.append("Краткое заметие:")
    lines.append("- Контекст обновлён автоматически.")
    lines.append("")
    lines.append("Ссылка(и):")
    for p in changed_files:
        lines.append(f"- https://github.com/{REPO}/blob/main/{p}")
    content = "\n".join(lines) + "\n"
    with open(fullpath, "w", encoding="utf-8") as f:
        f.write(content)
    return fname

def git_commit_and_push(files, message):
    try:
        subprocess.run(["git", "config", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "user.email", "github-actions@users.noreply.github.com"], check=True)
        subprocess.run(["git", "add"] + files, check=True)
        subprocess.run(["git", "commit", "-m", message], check=True)
        subprocess.run(["git", "push", "origin", "HEAD:main"], check=True)
        return True
    except subprocess.CalledProcessError as e:
        print("Git error:", e)
        return False

def main():
    changed = load_push_files()
    if not changed:
        print("No changed files found in event; exiting.")
        return
    ensure_context_exists()
    updated = False
    to_commit = []
    for p in changed:
        if re.search(r'(TotalNews.*\.py|templates/.*|sessions/aliases\.md|\.remember/.*|sessions/.*\.remember)$', p):
            ok = update_context_with_file(p)
            if ok:
                updated = True
                to_commit.append(CONTEXT_PATH)
    session_file = create_session_record(changed)
    to_commit.append(session_file)
    if not to_commit:
        print("Nothing to commit.")
        return
    msg = f"Auto-update CONTEXT and session for: {', '.join(changed)}"
    success = git_commit_and_push(to_commit, msg)
    if success:
        print("Committed and pushed:", to_commit)
    else:
        print("Failed to push changes.")

if __name__ == "__main__":
    main()
