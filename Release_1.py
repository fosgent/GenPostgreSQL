# new2.py
import os
import sys
import time
import tempfile
import re
import threading
import subprocess
import random
import string
from datetime import datetime
import tkinter as tk
from tkinter import messagebox
from tkinter.scrolledtext import ScrolledText
import ttkbootstrap as tb
from ttkbootstrap.constants import *
import psycopg2

__version__ = "1.0.1"
__author__ = "Евгений Сиротенко"
__description__ = "Программа для генерации и импорта тестовых баз PostgreSQL."

def write_version_file(version):
    try:
        if getattr(sys, 'frozen', False):
            path = os.path.join(os.path.dirname(sys.executable), "version.txt")
            with open(path, "w", encoding="utf-8") as f:
                f.write(version)
    except Exception:
        pass

class App(tb.Window):
    def __init__(self):
        super().__init__(themename="litera")
        self.title("Генератор базы PostgreSQL")
        self.geometry("1000x850+300+50")
        self.minsize(1000, 850)
        self.maxsize(1000, 1250)

        # --- Vars ---
        self.db_name_input = tk.StringVar()
        self.num_rows = tk.IntVar(value=1000)
        self.num_tables = tk.IntVar(value=1)
        self.template_display = tk.StringVar(value="Строки текста")

        self.host = tk.StringVar(value="localhost")
        self.port = tk.StringVar(value="5433")
        self.user = tk.StringVar(value="postgres")
        self.password = tk.StringVar(value="1234567")
        self.psql_path = tk.StringVar(value=r"C:/Program Files/PostgreSQL/17/bin/psql.exe")

        self.cancel_event = threading.Event()
        self.generated_file_path = None
        self.import_thread = None
        self.psql_process = None

        self.template_map = {
            "Строки текста": "text",
            "ID + Имя + Email": "user",
            "ID + Дата + Сумма": "order"
        }

        self._build_ui()

    # ---------------- UI ----------------
    def _build_ui(self):
        nb = tb.Notebook(self)
        nb.pack(fill=BOTH, expand=True, padx=20, pady=20)
        style = tb.Style()
        style.configure("TNotebook.Tab", cursor="hand2", padding=[20,10], font=("Arial",10), background="#dfe6e9", foreground="#2d3436")
        style.map("TNotebook.Tab", background=[("selected","#a7d4ff"), ("active", "#ffeaa7")], foreground=[("selected","#2d3436")])
        def on_motion(event):
            try:
                tab_index = nb.index(f"@{event.x},{event.y}")
                nb.config(cursor="hand2" if tab_index >= 0 else "")
            except Exception:
                nb.config(cursor="")
        nb.bind("<Motion>", on_motion)
        nb.bind("<Leave>", lambda e: nb.config(cursor=""))

        # Generation tab
        gen_tab = tb.Frame(nb)
        nb.add(gen_tab, text="Генерация")

        top = tb.Labelframe(gen_tab, text="Параметры генерации", padding=12)
        top.pack(fill="x", padx=8, pady=8)
        top.columnconfigure(1, weight=1)

        def add_row(r, label, widget):
            tb.Label(top, text=label).grid(row=r, column=0, sticky="w", padx=(0,12), pady=6)
            widget.grid(row=r, column=1, sticky="ew", pady=6)

        add_row(0, "Название базы (необязательно):", tb.Entry(top, textvariable=self.db_name_input))
        add_row(1, "Количество строк:", tb.Entry(top, textvariable=self.num_rows))
        add_row(2, "Количество таблиц:", tb.Entry(top, textvariable=self.num_tables))
        tb.Label(top, text="Шаблон таблицы:").grid(row=3, column=0, sticky="w", padx=(0,12), pady=6)
        tb.OptionMenu(top, self.template_display, *self.template_map.keys()).grid(row=3, column=1, sticky="w")

        # Buttons
        btns = tb.Frame(gen_tab)
        btns.pack(fill="x", padx=8, pady=6)
        for i in range(5):
            btns.columnconfigure(i, weight=1)

        self.start_button = tb.Button(btns, text="Сгенерировать и импортировать", command=self._start,
                                      bootstyle="success", cursor="hand2")
        self.start_button.grid(row=0, column=1, padx=6, pady=6, sticky="e")

        self.cancel_button = tb.Button(btns, text="Отмена", command=self._cancel, bootstyle="danger",
                                       cursor="arrow", state="disabled")
        self.cancel_button.grid(row=0, column=2, padx=6, pady=6)

        self.copylog_button = tb.Button(btns, text="Копировать лог", command=self._copy_log, bootstyle="warning")
        self.copylog_button.grid(row=0, column=3, padx=6, pady=6, sticky="w")

        self.about_button = tb.Button(btns, text="О программе", command=self._show_about, bootstyle="secondary")
        self.about_button.grid(row=0, column=4, padx=6, pady=6, sticky="w")

        # Progress & log
        prog_box = tb.Frame(gen_tab)
        prog_box.pack(fill="x", padx=10, pady=(8,0))
        self.progress = tb.Progressbar(prog_box, length=1000, bootstyle="info-striped")
        self.progress.pack(fill="x")
        self.progress_label = tb.Label(gen_tab, text="0%")
        self.progress_label.pack(pady=(4,0))

        self.log = ScrolledText(gen_tab, height=18, wrap="word", state="disabled")
        self.log.pack(fill="both", expand=True, padx=10, pady=10)

        # Connection tab
        conn_tab = tb.Frame(nb)
        nb.add(conn_tab, text="Подключение")
        conn_box = tb.Labelframe(conn_tab, text="Параметры подключения PostgreSQL", padding=12)
        conn_box.pack(fill="x", padx=8, pady=8)
        conn_box.columnconfigure(1, weight=1)
        def add_conn(r, label, widget):
            tb.Label(conn_box, text=label).grid(row=r, column=0, sticky="w", padx=(0,12), pady=6)
            widget.grid(row=r, column=1, sticky="ew", pady=6)
        add_conn(0, "Хост:", tb.Entry(conn_box, textvariable=self.host))
        add_conn(1, "Порт:", tb.Entry(conn_box, textvariable=self.port))
        add_conn(2, "Пользователь:", tb.Entry(conn_box, textvariable=self.user))
        add_conn(3, "Пароль:", tb.Entry(conn_box, textvariable=self.password, show="*"))
        add_conn(4, "Путь к psql.exe:", tb.Entry(conn_box, textvariable=self.psql_path))

        conn_btns = tb.Frame(conn_tab)
        conn_btns.pack(fill="x", padx=8, pady=6)
        conn_btns.columnconfigure(1, weight=1)
        self.test_button = tb.Button(conn_btns, text="Проверить подключение", command=self._test_connection,
                                     bootstyle="warning")
        self.test_button.grid(row=0, column=1, pady=6)

        # Footer
        self.footer_label = tb.Label(self, font=("Arial",9), foreground="gray")
        self.footer_label.pack(side="bottom", pady=(0,6))
        self._update_footer()

    # ---------------- Helpers / Log ----------------
    def _update_footer(self):
        self.footer_label.config(text=f"Версия: {__version__} | {datetime.now().strftime('%H:%M:%S')}")
        self.after(1000, self._update_footer)

    def _log(self, message, color=None, bold=False):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        text = f"[{ts}] {message}\n"
        self.log.config(state="normal")
        tag = None
        if color or bold:
            tag = f"tag_{color}_{'b' if bold else 'n'}"
            try:
                if not self.log.tag_names().__contains__(tag):
                    font = ("Arial", 10, "bold" if bold else "normal")
                    self.log.tag_config(tag, foreground=(color if color else "black"), font=font)
            except Exception:
                pass
            self.log.insert("end", text, tag)
        else:
            self.log.insert("end", text)
        self.log.see("end")
        self.log.config(state="disabled")

    def _copy_log(self):
        try:
            data = self.log.get(1.0, "end")
            self.clipboard_clear()
            self.clipboard_append(data)
            messagebox.showinfo("Скопировано", "Лог скопирован в буфер обмена.")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось скопировать лог: {e}")

    def _show_about(self):
        msg = f"{__description__}\n\nВерсия: {__version__}\nАвтор: {__author__}"
        if messagebox.askyesno("О программе", msg + "\n\nОткрыть страницу GitHub?"):
            import webbrowser
            webbrowser.open("https://github.com/fosgent/genpost")

    # ---------------- Start / Cancel ----------------
    def _start(self):
        if self.import_thread and self.import_thread.is_alive():
            messagebox.showwarning("Внимание", "Процесс уже запущен!")
            return
        self.cancel_event.clear()
        self._set_progress(0)
        self.log.config(state="normal"); self.log.delete(1.0, "end"); self.log.config(state="disabled")
        self._set_start_busy(True)
        self.import_thread = threading.Thread(target=self._generate_and_import, daemon=True)
        self.import_thread.start()

    def _cancel(self):
        if not (self.import_thread and self.import_thread.is_alive()):
            return
        self.cancel_event.set()
        self._log("Отмена запрошена.", color="red")
        if self.psql_process and self.psql_process.poll() is None:
            try:
                self.psql_process.terminate()
                try:
                    self.psql_process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    self.psql_process.kill()
                self._log("Процесс psql остановлен.", color="red")
            except Exception as e:
                self._log(f"Ошибка остановки psql: {e}", color="red")
        self._cleanup_file()
        db_name = self.db_name_input.get().strip() or f"PostgreSQL{self.num_rows.get()}rows"
        self._drop_database(db_name)
        self._set_progress(0)
        self._set_start_busy(False)

    def _set_start_busy(self, busy: bool):
        if busy:
            self.start_button.config(text="Подождите...", state="disabled")
            self.cancel_button.config(state="normal")
        else:
            self.start_button.config(text="Сгенерировать и импортировать", state="normal")
            self.cancel_button.config(state="disabled")

    def _set_progress(self, value):
        value = max(0, min(100, int(value)))
        self.progress['value'] = value
        self.progress_label.config(text=f"{value}%")
        self.update_idletasks()

    def _check_cancel(self):
        if self.cancel_event.is_set():
            raise RuntimeError("cancelled")

    # ---------------- Main flow ----------------
    def _generate_and_import(self):
        try:
            num_rows = int(self.num_rows.get())
            num_tables = int(self.num_tables.get())
            template = self.template_map.get(self.template_display.get(), "text")
            db_name = self.db_name_input.get().strip() or f"PostgreSQL{num_rows}rows"
            psql_exe = self.psql_path.get()

            out_dir = tempfile.gettempdir()
            filename = f"PostgreSQL{num_rows}rows.sql"
            filepath = os.path.join(out_dir, filename)
            self.generated_file_path = filepath

            if num_rows <= 0 or num_tables <= 0 or not os.path.isfile(psql_exe):
                messagebox.showerror("Ошибка", "Проверьте параметры и путь к psql.exe")
                self._set_start_busy(False)
                return

            # GENERATION: 0..80%
            self._log(f"Генерируем файл {filename}... (строк: {num_rows}, таблиц: {num_tables}, шаблон: {self.template_display.get()})")
            gen_start = time.time()
            self._generate_sql_file(filepath, num_rows, num_tables, template)
            gen_elapsed = time.time() - gen_start
            self._check_cancel()

            size_mb = os.path.getsize(filepath) / (1024*1024)
            self._log(f"Генерация завершена. Всего строк: {num_rows}. Время генерации: {gen_elapsed:.2f} сек.")
            self._log(f"Размер сгенерированного файла: {size_mb:.2f} MB")
            self._log(f"Файл расположен: {filepath}")

            # CREATE DB
            self._log(f"Создаём базу данных {db_name}...")
            if not self._create_database(db_name):
                self._log("Ошибка создания базы данных.", color="red")
                self._set_start_busy(False)
                return
            self._check_cancel()
            self._log("База создана.")

            # IMPORT: 80..100%
            import_start = time.time()
            total_count, table_count = self._import_sql(db_name, filepath)
            import_elapsed = time.time() - import_start
            self._check_cancel()

            # database real size
            real_size_mb = None
            try:
                conn = psycopg2.connect(dbname=db_name, user=self.user.get(), password=self.password.get(),
                                        host=self.host.get(), port=self.port.get())
                cur = conn.cursor()
                cur.execute("SELECT pg_database_size(%s);", (db_name,))
                bytes_size = cur.fetchone()[0]
                real_size_mb = round(bytes_size / (1024*1024), 2)
                cur.close(); conn.close()
                self._log(f"Реальный размер базы: {real_size_mb} MB")
            except Exception as e:
                self._log(f"Ошибка при получении размера базы: {e}", color="red")

            self._cleanup_file()

            final_size = f"{real_size_mb} MB" if real_size_mb is not None else "неизвестно"
            self._log(f"ИТОГ: вставлено {total_count} строк в {table_count} таблиц. Размер базы '{db_name}': {final_size}",
                      color="green", bold=True)

        except RuntimeError:
            pass
        except Exception as e:
            self._log(f"Ошибка: {e}", color="red")
        finally:
            self._set_start_busy(False)
            if not self.cancel_event.is_set():
                self._set_progress(100)

    # ---------------- Generation routine ----------------
    def _generate_sql_file(self, filepath, total_rows, num_tables, template):
        target_start, target_end = 0, 80
        total_written = 0
        base_rows = total_rows // num_tables
        extra_rows = total_rows % num_tables

        with open(filepath, "w", encoding="utf-8") as f:
            for t in range(1, num_tables+1):
                self._check_cancel()
                table_name = f"table_{t}"
                if template == "text":
                    f.write(f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, content TEXT);\n")
                elif template == "user":
                    f.write(f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, name VARCHAR(100), email VARCHAR(100));\n")
                elif template == "order":
                    f.write(f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, order_date DATE, total NUMERIC(10,2));\n")
                else:
                    f.write(f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, content TEXT);\n")

                rows_in_table = base_rows + (1 if t <= extra_rows else 0)
                self._log(f"{table_name}: {rows_in_table} строк")

                chunk_size = 2000
                written = 0
                while written < rows_in_table:
                    self._check_cancel()
                    take = min(chunk_size, rows_in_table - written)
                    vals = []
                    for _ in range(take):
                        if template == "text":
                            text_sample = ''.join(random.choices(string.ascii_letters + " ", k=50)).replace("'", "''")
                            vals.append(f"(DEFAULT, '{text_sample}')")
                        elif template == "user":
                            name = ''.join(random.choices(string.ascii_letters, k=7))
                            email = name.lower() + "@example.com"
                            vals.append(f"(DEFAULT, '{name}', '{email}')")
                        elif template == "order":
                            date_str = f"2025-07-{random.randint(1,28):02d}"
                            total_val = round(random.uniform(10, 1000), 2)
                            vals.append(f"(DEFAULT, '{date_str}', {total_val})")
                        else:
                            vals.append(f"(DEFAULT, 'sample text')")
                    f.write(f"INSERT INTO {table_name} VALUES {', '.join(vals)};\n")
                    written += take
                    total_written += take
                    if total_rows > 0:
                        frac = total_written / total_rows
                        prog = target_start + (target_end - target_start) * frac
                        self._set_progress(prog)
        self._set_progress(80)
        self._log("Генерация SQL завершена.")

    # ---------------- Create DB ----------------
    def _create_database(self, db_name):
        host = self._resolve_host(self.host.get())
        env = os.environ.copy()
        env["PGPASSWORD"] = self.password.get()
        psql = self.psql_path.get()
        port, user = self.port.get(), self.user.get()
        try:
            subprocess.run([psql, f"--host={host}", f"--port={port}", f"--username={user}", "-d", "postgres",
                            "-c", f'DROP DATABASE IF EXISTS "{db_name}";'], capture_output=True, text=True, env=env)
            res = subprocess.run([psql, f"--host={host}", f"--port={port}", f"--username={user}", "-d", "postgres",
                                  "-c", f'CREATE DATABASE "{db_name}";'], capture_output=True, text=True, env=env)
            if res.returncode != 0:
                self._log(f"Ошибка создания БД: {res.stderr.strip()}", color="red")
                return False
            return True
        except Exception as e:
            self._log(f"Ошибка создания БД: {e}", color="red")
            return False

    # ---------------- Import SQL ----------------
    def _import_sql(self, db_name, filepath):
        host = self._resolve_host(self.host.get())
        env = os.environ.copy()
        env["PGPASSWORD"] = self.password.get()
        psql = self.psql_path.get()
        port, user = self.port.get(), self.user.get()

        table_names = []
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if s.upper().startswith("CREATE TABLE"):
                        m = re.search(r'CREATE TABLE IF NOT EXISTS\s+"?([^\s"(]+)"?', s, re.IGNORECASE)
                        if m:
                            table_names.append(m.group(1))
        except Exception as e:
            self._log(f"Не удалось прочитать SQL-файл: {e}", color="red")

        try:
            self.psql_process = subprocess.Popen([psql, f"--host={host}", f"--port={port}",
                                                  f"--username={user}", "-d", db_name, "-f", filepath],
                                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                                 text=True, bufsize=1, env=env)
        except Exception as e:
            self._log(f"Не удалось запустить psql: {e}", color="red")
            return 0, len(table_names)

        cur_prog = 80.0
        last_tick = time.time()
        try:
            for raw in self.psql_process.stdout:
                self._check_cancel()
                now = time.time()
                if now - last_tick >= 0.05 and cur_prog < 99:
                    cur_prog += 0.4
                    self._set_progress(cur_prog)
                    last_tick = now
            self.psql_process.wait()
        except RuntimeError:
            if self.psql_process and self.psql_process.poll() is None:
                try:
                    self.psql_process.terminate()
                except Exception:
                    pass
        finally:
            self.psql_process = None

        if cur_prog < 99:
            self._set_progress(99)

        total_count = 0
        for t in table_names:
            self._check_cancel()
            try:
                res = subprocess.run([psql, f"--host={host}", f"--port={self.port.get()}", f"--username={self.user.get()}",
                                      "-d", db_name, "-t", "-c", f'SELECT COUNT(*) FROM "{t}";'],
                                     capture_output=True, text=True, env=env)
                count_str = (res.stdout or "").strip()
                count = int(count_str) if count_str else 0
                total_count += count
            except Exception:
                pass
        self._set_progress(100)
        return total_count, len(table_names)

    # ---------------- Drop DB ----------------
    def _drop_database(self, db_name):
        host = self._resolve_host(self.host.get())
        env = os.environ.copy()
        env["PGPASSWORD"] = self.password.get()
        psql = self.psql_path.get()
        try:
            subprocess.run([psql, f"--host={host}", f"--port={self.port.get()}", f"--username={self.user.get()}",
                            "-d", "postgres", "-c", f'DROP DATABASE IF EXISTS "{db_name}";'],
                           capture_output=True, text=True, env=env)
        except Exception as e:
            self._log(f"Ошибка удаления базы: {e}", color="red")

    # ---------------- Cleanup file ----------------
    def _cleanup_file(self):
        path = self.generated_file_path
        if not path:
            return
        for attempt in range(5):
            try:
                if os.path.exists(path):
                    os.remove(path)
                    self._log(f"Временный файл {path} удалён.")
                self.generated_file_path = None
                return
            except Exception as e:
                if attempt == 4:
                    self._log(f"Ошибка удаления временного файла: {e}", color="red")
                time.sleep(0.25)

    # ---------------- Test connection ----------------
    def _test_connection(self):
        try:
            conn = psycopg2.connect(dbname="postgres", user=self.user.get(), password=self.password.get(),
                                    host=self.host.get(), port=self.port.get())
            conn.close()
            messagebox.showinfo("Подключение", "Соединение успешно!")
        except Exception as e:
            messagebox.showerror("Ошибка подключения", f"{e}")

    # ---------------- Host resolve ----------------
    def _resolve_host(self, host):
        if not host:
            return "127.0.0.1"
        if host.lower() in ("localhost", "127.0.0.1"):
            return "127.0.0.1"
        return host

if __name__ == "__main__":
    write_version_file(__version__)
    app = App()
    app.mainloop()
