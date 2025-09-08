import os
import sys
import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter.scrolledtext import ScrolledText
import ttkbootstrap as tb
from ttkbootstrap.constants import *
import subprocess
import threading
import random
import string
from datetime import datetime
import socket
import psycopg2
import webbrowser

__version__ = "1.1.11"
__author__ = "Евгений Сиротенко"
__description__ = "Программа для генерации и импорта тестовых баз PostgreSQL."

def get_app_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

def write_version_file(version):
    try:
        if getattr(sys, 'frozen', False):
            version_path = os.path.join(get_app_dir(), "version.txt")
            with open(version_path, "w", encoding="utf-8") as f:
                f.write(version)
    except Exception as e:
        print(f"Ошибка записи version.txt: {e}")

class App(tb.Window):
    def __init__(self):
        super().__init__(themename="litera")
        self.title("Генератор базы PostgreSQL")
        self.geometry("1000x850+300+50")
        self.minsize(1000, 850)
        self.maxsize(1000, 1400)

        # ---------------- Переменные -----------------
        self.db_name_input = tb.StringVar()
        self.num_rows = tb.IntVar(value=1000)  # количество строк
        self.num_tables = tb.IntVar(value=1)
        self.table_template = tb.StringVar(value="text")
        self.template_display = tb.StringVar(value="Строки текста")

        self.host = tb.StringVar(value="localhost")
        self.port = tb.StringVar(value="5433")
        self.user = tb.StringVar(value="postgres")
        self.password = tb.StringVar(value="1234567")
        self.psql_path = tb.StringVar(value=r"C:/Program Files/PostgreSQL/17/bin/psql.exe")

        self.cancel_event = threading.Event()
        self.generated_file_path = None
        self.import_thread = None
        self.psql_process = None

        self.template_map = {
            "Строки текста": "text",
            "ID + Имя + Email": "user",
            "ID + Дата + Сумма": "order"
        }

        self.can_cancel = False
        self._build_ui()

    # ---------------- UI -----------------
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

        # --- Вкладка Генерация ---
        gen_tab = tb.Frame(nb)
        nb.add(gen_tab, text="Генерация")

        top = tb.Labelframe(gen_tab, text="Параметры генерации", padding=12)
        top.pack(fill=X, padx=8, pady=8)

        def add_row(r, label, widget):
            tb.Label(top, text=label).grid(row=r, column=0, sticky=W, padx=(0, 12), pady=6)
            widget.grid(row=r, column=1, sticky=EW, pady=6)
        top.columnconfigure(1, weight=1)

        add_row(0, "Название базы (необязательно):", tb.Entry(top, textvariable=self.db_name_input))
        add_row(1, "Количество строк:", tb.Entry(top, textvariable=self.num_rows))
        add_row(2, "Количество таблиц:", tb.Entry(top, textvariable=self.num_tables))

        tb.Label(top, text="Шаблон таблицы:").grid(row=3, column=0, sticky=W, padx=(0,12), pady=6)
        opt = tb.OptionMenu(top, self.template_display, *self.template_map.keys())
        opt.grid(row=3, column=1, sticky=W, pady=6)

        # Кнопки управления
        btns = tb.Frame(gen_tab)
        btns.pack(fill=X, padx=8, pady=6)
        for i in range(5):
            btns.columnconfigure(i, weight=1)

        self.start_button = tb.Button(btns, text="Сгенерировать и импортировать", width=30,
                                      command=self._start, bootstyle="success", cursor="hand2")
        self.start_button.grid(row=0, column=1, padx=6, pady=6, sticky=E)

        self.cancel_button = tb.Button(btns, text="Отмена", command=self._cancel, bootstyle="danger",
                                       cursor="arrow", state=DISABLED)
        self.cancel_button.grid(row=0, column=2, padx=6, pady=6)

        self.copylog_button = tb.Button(btns, text="Копировать лог", command=self._copy_log,
                                        bootstyle="warning", cursor="hand2")
        self.copylog_button.grid(row=0, column=3, padx=6, pady=6, sticky=W)

        self.about_button = tb.Button(btns, text="О программе", command=self._show_about,
                                      bootstyle="secondary", cursor="hand2")
        self.about_button.grid(row=0, column=4, padx=6, pady=6, sticky=W)

        # Прогресс и лог
        prog_box = tb.Frame(gen_tab)
        prog_box.pack(fill=X, padx=10, pady=(8,0))
        self.progress = tb.Progressbar(prog_box, length=1000, bootstyle="info-striped")
        self.progress.pack(fill=X)
        self.progress_label = tb.Label(gen_tab, text="0%")
        self.progress_label.pack(pady=(4,0))

        self.log = ScrolledText(gen_tab, height=18, wrap="word", state=tk.DISABLED)
        self.log.pack(fill=BOTH, expand=True, padx=10, pady=10)

        # --- Вкладка Подключение ---
        conn_tab = tb.Frame(nb)
        nb.add(conn_tab, text="Подключение")

        conn_box = tb.Labelframe(conn_tab, text="Параметры подключения PostgreSQL", padding=12)
        conn_box.pack(fill=X, padx=8, pady=8)

        def add_conn_row(r, label, widget):
            tb.Label(conn_box, text=label).grid(row=r, column=0, sticky=W, padx=(0,12), pady=6)
            widget.grid(row=r, column=1, sticky=EW, pady=6)
        conn_box.columnconfigure(1, weight=1)

        add_conn_row(0, "Хост:", tb.Entry(conn_box, textvariable=self.host))
        add_conn_row(1, "Порт:", tb.Entry(conn_box, textvariable=self.port))
        add_conn_row(2, "Пользователь:", tb.Entry(conn_box, textvariable=self.user))
        add_conn_row(3, "Пароль:", tb.Entry(conn_box, textvariable=self.password, show="*"))
        add_conn_row(4, "Путь к psql.exe:", tb.Entry(conn_box, textvariable=self.psql_path))

        conn_btns = tb.Frame(conn_tab)
        conn_btns.pack(fill=X, padx=8, pady=6)
        for i in range(3):
            conn_btns.columnconfigure(i, weight=1)

        self.test_button = tb.Button(conn_btns, text="Проверить подключение", command=self._test_connection,
                                     bootstyle="warning", cursor="hand2", width=25)
        self.test_button.grid(row=0, column=1, pady=6)

        # Footer
        self.footer_label = tb.Label(self, font=("Arial",9), foreground="gray")
        self.footer_label.pack(side=BOTTOM, pady=(0,6))

        def _update_footer_time():
            current_time = datetime.now().strftime("%H:%M:%S")
            self.footer_label.config(text=f"Версия: {__version__} | {current_time}")
            self.after(1000, _update_footer_time)
        _update_footer_time()

    # ---------------- Лог -----------------
    def _log(self, message):
        message = message.strip()
        if not message:
            return
        self.log.config(state=tk.NORMAL)
        self.log.insert(tk.END, f"{message}\n")
        self.log.see(tk.END)
        self.log.config(state=tk.DISABLED)

    def _copy_log(self):
        try:
            text = self.log.get(1.0, tk.END)
            self.clipboard_clear()
            self.clipboard_append(text)
            messagebox.showinfo("Скопировано", "Лог скопирован в буфер обмена.")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось скопировать лог: {e}")

    def _show_about(self):
        msg = f"{__description__}\n\nВерсия: {__version__}\nАвтор: {__author__}"
        if messagebox.askyesno("О программе", msg + "\n\nОткрыть страницу GitHub для проверки обновлений?"):
            webbrowser.open("https://github.com/fosgent/genpost")

    # ---------------- Старт процесса -----------------
    def _start(self):
        if self.import_thread and self.import_thread.is_alive():
            messagebox.showwarning("Внимание", "Процесс уже запущен!")
            return
        self.cancel_event.clear()
        self.progress['value'] = 0
        self.progress_label.config(text="0%")
        self.log.config(state=tk.NORMAL)
        self.log.delete(1.0, tk.END)
        self.log.config(state=tk.DISABLED)
        self._set_start_button_busy(True)
        self.import_thread = threading.Thread(target=self._generate_and_import, daemon=True)
        self.import_thread.start()

    # ---------------- Кнопка Отмена -----------------
    def _cancel(self):
        if not self.import_thread or not self.import_thread.is_alive():
            return
        self.cancel_event.set()
        self._log("Отмена запрошена.")

        if self.psql_process and self.psql_process.poll() is None:
            try:
                self.psql_process.terminate()
                self._log("Импорт остановлен.")
            except Exception as e:
                self._log(f"Ошибка остановки процесса: {e}")

        self._cleanup_file()
        db_name = self.db_name_input.get().strip() or f"PostgreSQL{self.num_rows.get()}rows"
        self._drop_database(db_name)

        self.progress['value'] = 0
        self.progress_label.config(text="0%")
        self._set_start_button_busy(False)

    # ---------------- Set busy -----------------
    def _set_start_button_busy(self, busy: bool):
        if busy:
            self.start_button.config(text="Подождите...", state=DISABLED, cursor="arrow")
            self.cancel_button.config(state=NORMAL, cursor="hand2")
            self.can_cancel = True
        else:
            self.start_button.config(text="Сгенерировать и импортировать", state=NORMAL, cursor="hand2")
            self.cancel_button.config(state=DISABLED, cursor="arrow")
            self.can_cancel = False

    # ---------------- Проверка отмены -----------------
    def _check_cancel(self):
        if self.cancel_event.is_set():
            self.progress['value'] = 0
            self.progress_label.config(text="0%")
            self._set_start_button_busy(False)
            self._cleanup_file()
            db_name = self.db_name_input.get().strip() or f"PostgreSQL{self.num_rows.get()}rows"
            self._drop_database(db_name)
            raise RuntimeError("cancelled")

    # ---------------- Генерация и импорт -----------------
    def _generate_and_import(self):
        try:
            num_rows = self.num_rows.get()
            num_tables = self.num_tables.get()
            template_key = self.template_display.get()
            template = self.template_map.get(template_key, "text")
            db_name = self.db_name_input.get().strip() or f"PostgreSQL{num_rows}rows"
            psql_exe = self.psql_path.get()

            output_dir = r"C:\Users\evsir\AppData\Local\Temp"
            filename = f"PostgreSQL{num_rows}rows.sql"
            filepath = os.path.join(output_dir, filename)
            self.generated_file_path = filepath

            if num_rows <= 0 or num_tables <= 0 or not os.path.isfile(psql_exe):
                messagebox.showerror("Ошибка", "Проверьте параметры и пути")
                self._set_start_button_busy(False)
                return

            self._log(f"Генерируем файл {filename}...")
            self._generate_sql_file(filepath, num_rows, num_tables, template)
            self._check_cancel()
            self._log(f"Файл сгенерирован: {filepath}")

            self._log(f"Создаём базу данных {db_name}...")
            self.progress['value'] = 70
            self.progress_label.config(text="70%")
            self.update_idletasks()
            self._check_cancel()
            if not self._create_database(db_name):
                self._log("Ошибка создания базы данных.")
                return
            self._check_cancel()
            self.progress['value'] = 80
            self.progress_label.config(text="80%")
            self.update_idletasks()

            self._log("Начинаем импорт...")
            if not self._import_sql(db_name, filepath):
                self._log("Импорт завершился с ошибкой.")
                return

            try:
                conn = psycopg2.connect(
                    dbname=db_name,
                    user=self.user.get(),
                    password=self.password.get(),
                    host=self.host.get(),
                    port=self.port.get()
                )
                cur = conn.cursor()
                cur.execute(f"SELECT pg_size_pretty(pg_database_size('{db_name}'));")
                real_size = cur.fetchone()[0]
                self._log(f"Реальный размер базы: {real_size}")
                cur.close()
                conn.close()
            except Exception as e:
                self._log(f"Ошибка при получении размера базы: {e}")

            self.progress['value'] = 100
            self.progress_label.config(text="100%")
            self._cleanup_file()
            self._log(f"База данных '{db_name}' успешно создана и импортирована.")
            self._log("Процесс завершён.")

        except RuntimeError as e:
            if str(e) == "cancelled":
                self._log("Процесс отменён пользователем.")
        except Exception as e:
            self._log(f"Ошибка: {e}")
        finally:
            self._set_start_button_busy(False)

    # ---------------- SQL генерация -----------------
    def _generate_sql_file(self, filepath, num_rows, num_tables, template):
        total_rows_inserted = 0
        row_overhead = 23
        toast_factor = 1.1 if template == "text" else 1.0

        with open(filepath, "w", encoding="utf-8") as f:
            for t in range(1, num_tables + 1):
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

                while total_rows_inserted < num_rows:
                    self._check_cancel()
                    batch_size = min(1000, num_rows - total_rows_inserted)
                    current_batch = []

                    for _ in range(batch_size):
                        if template == "text":
                            text_sample = ''.join(random.choices(string.ascii_letters + " ", k=50)).replace("'", "''")
                            current_batch.append(f"(DEFAULT, '{text_sample}')")
                        elif template == "user":
                            name = ''.join(random.choices(string.ascii_letters, k=7))
                            email = name.lower() + "@example.com"
                            current_batch.append(f"(DEFAULT, '{name}', '{email}')")
                        elif template == "order":
                            date_str = f"2025-07-{random.randint(1,28):02d}"
                            total = round(random.uniform(10, 1000), 2)
                            current_batch.append(f"(DEFAULT, '{date_str}', {total})")
                        else:
                            current_batch.append(f"(DEFAULT, 'sample text')")
                        total_rows_inserted += 1

                    f.write(f"INSERT INTO {table_name} VALUES {', '.join(current_batch)};\n")

                    progress_value = min(70, (total_rows_inserted / num_rows) * 70)
                    self.progress['value'] = progress_value
                    self.progress_label.config(text=f"{int(progress_value)}%")
                    self.update_idletasks()

        self._log(f"Генерация завершена. Всего строк: {total_rows_inserted}")

    # ---------------- Создание базы -----------------
    def _create_database(self, db_name):
        host = self._resolve_host(self.host.get())
        env = os.environ.copy()
        env["PGPASSWORD"] = self.password.get()
        psql = self.psql_path.get()
        port, user = self.port.get(), self.user.get()
        try:
            subprocess.run([psql, f"--host={host}", f"--port={port}", f"--username={user}", "-d", "postgres",
                            "-c", f'DROP DATABASE IF EXISTS "{db_name}";'],
                           capture_output=True, text=True, env=env)
            result = subprocess.run([psql, f"--host={host}", f"--port={port}", f"--username={user}", "-d", "postgres",
                            "-c", f'CREATE DATABASE "{db_name}";'],
                            capture_output=True, text=True, env=env)
            if result.returncode != 0:
                self._log(result.stderr)
                return False
            return True
        except Exception as e:
            self._log(f"Ошибка создания базы: {e}")
            return False

    # ---------------- Импорт SQL -----------------
    def _import_sql(self, db_name, filepath):
        host = self._resolve_host(self.host.get())
        env = os.environ.copy()
        env["PGPASSWORD"] = self.password.get()
        psql = self.psql_path.get()
        port, user = self.port.get(), self.user.get()
        try:
            self.psql_process = subprocess.Popen([psql, f"--host={host}", f"--port={port}", f"--username={user}",
                                                 "-d", db_name, "-f", filepath],
                                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
            while True:
                line = self.psql_process.stdout.readline()
                if not line:
                    break
                self._log(line.strip())
                self.update_idletasks()
                self._check_cancel()
            self.psql_process.wait()
            self.psql_process = None
            return True
        except RuntimeError:
            return False
        except Exception as e:
            self._log(f"Ошибка импорта: {e}")
            return False

    # ---------------- Drop базы -----------------
    def _drop_database(self, db_name):
        host = self._resolve_host(self.host.get())
        env = os.environ.copy()
        env["PGPASSWORD"] = self.password.get()
        psql = self.psql_path.get()
        port, user = self.port.get(), self.user.get()
        try:
            subprocess.run([psql, f"--host={host}", f"--port={port}", f"--username={user}", "-d", "postgres",
                            "-c", f'DROP DATABASE IF EXISTS "{db_name}";'],
                           capture_output=True, text=True, env=env)
        except Exception as e:
            self._log(f"Ошибка удаления базы: {e}")

    # ---------------- Удаление временного файла -----------------
    def _cleanup_file(self):
        try:
            if self.generated_file_path and os.path.exists(self.generated_file_path):
                os.remove(self.generated_file_path)
                self._log(f"Временный файл {self.generated_file_path} удалён.")
                self.generated_file_path = None
        except Exception as e:
            self._log(f"Ошибка удаления файла: {e}")

    # ---------------- Тест подключения -----------------
    def _test_connection(self):
        try:
            conn = psycopg2.connect(
                dbname="postgres",
                user=self.user.get(),
                password=self.password.get(),
                host=self.host.get(),
                port=self.port.get()
            )
            conn.close()
            messagebox.showinfo("Подключение", "Соединение успешно!")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось подключиться: {e}")

    # ---------------- Хост -----------------
   
    def _resolve_host(self, host):
        if host.lower() in ("localhost", "127.0.0.1"):
            return "127.0.0.1"
        return host


if __name__ == "__main__":
    write_version_file(__version__)
    app = App()
    app.mainloop()
