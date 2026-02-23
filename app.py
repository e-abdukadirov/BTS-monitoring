import pandas as pd
import requests
from io import BytesIO
import zipfile
import os
import json
import threading
import urllib3
import re
import traceback
import pickle  # <-- Добавлено для сохранения входа
import ftplib
from bs4 import BeautifulSoup
from datetime import datetime
from flask import Flask, render_template_string, redirect, url_for, request, send_file, jsonify

# --- [БЛОК 1: НАСТРОЙКИ СЕРВЕРА] ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)  # Сначала создаем приложение
app.config['APP_NAME'] = "Excell to SMS text"
CONFIG_FILE = "user_config.json"
COOKIE_FILE = "session_cookies.pkl"  # Файл для запоминания входа

PORT = 5007
BASE_URL = "https://pm.utc.uz"
# Background image path
BACKGROUND_IMAGE_PATH = r"c:\Users\elyor\Desktop\Excell\fon\—Pngtree—contemporary dining and kitchen area_8837825.jpg"
# --- [БЛОК 2: НАСТРОЙКИ КОЛОНОК И РЕГИОНОВ] ---
COLUMN_LOOKUP = {
    "hw_id": ["Alarm Source", "MO Name", "NE Name", "Source"],
    "hw_msg": ["Name", "Alarm Name", "Event Type"],
    "zt_id": ["Site Name(Office)", "NE Name", "Site ID", "Managed Element"],
    "zt_msg": ["Alarm Code Name", "Alarm Name", "Specific Problem"]
}

REGIONS_MAP = {
    "Andijon viloyati": "Андижанская обл.",
    "Toshkent viloyati": "Ташкентская обл.",
    "Toshkent shahri": "г. Ташкент.",
    "Surxondaryo viloyati": "Сурхандарьинская обл.",
    "Samarqand viloyati": "Самаркандская обл.",
    "Namangan viloyati": "Наманганская обл.",
    "Farg‘ona viloyati": "Ферганская обл.",
    "Buxoro viloyati": "Бухарская обл.",
    "Jizzax viloyati": "Джизакская обл.",
    "Xorazm viloyati": "Хорезмская обл.",
    "Qashqadaryo viloyati": "Кашкадарьинская обл.",
    "Qoraqalpog‘iston Respublikasi": "Респ. Каракалпакстан.",
    "Navoiy viloyati": "Навоийская обл.",
    "Sirdaryo viloyati": "Сырдарьинская обл."
}

# Dashboard HTML template (integrated, no separate app needed!)
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PM UTC Dashboard | Live Data</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Inter', sans-serif; background-color: #0f172a; color: #e2e8f0; }
        .card { background: #1e293b; border-radius: 12px; padding: 20px; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1); }
        .stat-value { font-size: 2.5rem; font-weight: 700; background: linear-gradient(to right, #38bdf8, #818cf8); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .scroller::-webkit-scrollbar { width: 8px; height: 8px; }
        .scroller::-webkit-scrollbar-track { background: #1e293b; }
        .scroller::-webkit-scrollbar-thumb { background: #475569; border-radius: 4px; }
    </style>
</head>
<body class="p-6 max-w-[1800px] mx-auto">
    <div class="flex justify-between items-center mb-8">
        <div>
            <h1 class="text-3xl font-bold text-white">Project Progress Dashboard</h1>
            <p class="text-slate-400 text-sm">Target: Base Station Rollout 2025-2026</p>
        </div>
        <div class="flex gap-4">
            <select id="vendorSelect" class="bg-slate-700 text-white px-4 py-2 rounded-lg border border-slate-600" onchange="fetchData()">
                <option value="all">All Vendors</option>
                <option value="Huawei">Huawei</option>
                <option value="ZTE">ZTE</option>
            </select>
            <button id="refreshBtn" onclick="triggerRefresh()" class="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg font-semibold">🔄 Refresh</button>
            <button class="bg-cyan-500 hover:bg-cyan-600 text-white px-4 py-2 rounded-lg font-semibold" onclick="showGraphModal()">📈 Create Grafic</button>
            <button onclick="window.close()" class="bg-gray-600 hover:bg-gray-700 text-white px-4 py-2 rounded-lg">Close</button>
        </div>
    </div>

    <div class="grid grid-cols-1 md:grid-cols-5 gap-6 mb-8">
        <div class="card border-l-4 border-blue-500">
            <h3 class="text-slate-400 text-xs font-bold uppercase">Total Sites</h3>
            <div class="stat-value" id="totalVal">0</div>
        </div>
        <div class="card border-l-4 border-green-500">
            <h3 class="text-slate-400 text-xs font-bold uppercase">Completed MOD</h3>
            <div class="stat-value" id="doneModVal" style="-webkit-text-fill-color: #4ade80;">0</div>
        </div>
        <div class="card border-l-4 border-emerald-400">
            <h3 class="text-slate-400 text-xs font-bold uppercase">Completed NEW</h3>
            <div class="stat-value" id="doneNewVal" style="-webkit-text-fill-color: #34d399;">0</div>
        </div>
        <div class="card border-l-4 border-yellow-500">
            <h3 class="text-slate-400 text-xs font-bold uppercase">In Progress</h3>
            <div class="stat-value" id="inpVal" style="-webkit-text-fill-color: #facc15;">0</div>
        </div>
        <div class="card border-l-4 border-purple-500">
            <h3 class="text-slate-400 text-xs font-bold uppercase">Progress</h3>
            <div class="stat-value" id="progVal" style="-webkit-text-fill-color: #c084fc;">0%</div>
        </div>
    </div>

    <div class="flex gap-6 transition-all duration-300">
        <!-- LEFT: Stats Table -->
        <div id="stats-panel" class="card w-full transition-all duration-300">
            <h3 class="text-xl font-semibold text-white mb-4">Status by Region</h3>
            <div class="overflow-x-auto scroller max-h-[700px]">
                <table class="w-full text-left text-sm text-slate-300">
                    <thead class="text-xs uppercase bg-slate-800 text-slate-200 sticky top-0">
                        <tr>
                            <th class="px-4 py-3 bg-slate-800">Region</th>
                            <th class="px-4 py-3 bg-slate-800 w-1/4">Progress</th>
                            <th class="px-4 py-3 text-center bg-slate-800 text-blue-300">Mod</th>
                            <th class="px-4 py-3 text-center bg-slate-800 text-purple-300">New</th>
                            <th class="px-4 py-3 text-right bg-slate-800 text-green-400">Done MOD</th>
                            <th class="px-4 py-3 text-right bg-slate-800 text-emerald-400">Done NEW</th>
                            <th class="px-4 py-3 text-right bg-slate-800 text-yellow-400">In Prog</th>
                            <th class="px-4 py-3 text-right bg-slate-800 text-cyan-400">SMS</th>
                            <th class="px-4 py-3 text-right bg-slate-800 text-slate-400">New</th>
                            <th class="px-4 py-3 text-right bg-slate-800 font-bold">Total</th>
                        </tr>
                    </thead>
                    <tbody id="regionTableBody" class="divide-y divide-slate-700"></tbody>
                </table>
            </div>
        </div>

        <!-- RIGHT: Graph Panel (Hidden by default) -->
        <div id="graph-side-panel" class="hidden card w-1/3 transition-all duration-300 flex flex-col" style="display:none;">
            <div class="flex justify-between items-center mb-4">
                <h3 class="text-lg font-semibold text-white">📈 Graphics</h3>
                <div class="flex gap-2">
                    <select id="graph-period" onchange="loadGraphData()" class="bg-slate-700 text-white text-xs px-2 py-1 rounded border border-slate-600">
                        <option value="day">Daily</option>
                        <option value="week">Weekly</option>
                        <option value="month">Monthly</option>
                    </select>
                    <button onclick="toggleGraphView()" class="text-slate-400 hover:text-white">✖</button>
                </div>
            </div>
            <div class="flex-1 relative min-h-[400px]">
                <canvas id="progressChart"></canvas>
            </div>
        </div>
    </div>

    <!-- Chart JS -->
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script>
        let myChart = null;
        let isGraphOpen = false;

        // --- CORE DATA FUNCTIONS ---

        async function triggerRefresh() {
            const btn = document.getElementById('refreshBtn');
            const originalText = btn.innerHTML;
            btn.innerHTML = '⏳ Refreshing...';
            btn.disabled = true;

            try {
                const response = await fetch('/api/refresh_data', {method: 'POST'});
                const data = await response.json();

                if(data.status === 'ok') {
                    await fetchData();
                    alert(data.msg);
                    // Reload graph if it is currently open
                    if(isGraphOpen) {
                        loadGraphData(); 
                    }
                } else {
                    alert('Error: ' + data.msg);
                }
            } catch (e) {
                console.error("Error refreshing:", e);
                alert("Error refreshing data");
            } finally {
                btn.innerHTML = originalText;
                btn.disabled = false;
            }
        }

        async function fetchData() {
            const vendorSelect = document.getElementById('vendorSelect');
            const vendor = vendorSelect ? vendorSelect.value : 'all';

            try {
                const response = await fetch(`/api/dashboard_stats?vendor=${vendor}`);
                const data = await response.json();
                if(data.error) { alert(data.error); return; }
                updateUI(data);
            } catch (e) {
                console.error("Error fetching data:", e);
            }
        }

        function updateUI(data) {
            document.getElementById('totalVal').innerText = data.summary.total;
            document.getElementById('doneModVal').innerText = data.summary.done_mod;
            document.getElementById('doneNewVal').innerText = data.summary.done_new;
            document.getElementById('inpVal').innerText = data.summary.inprogress;
            document.getElementById('progVal').innerText = data.summary.progress + '%';

            const tbody = document.getElementById('regionTableBody');
            tbody.innerHTML = '';

            data.regions.forEach(reg => {
                let color = reg.progress < 15 ? 'bg-rose-500' : reg.progress > 75 ? 'bg-emerald-500' : reg.progress >= 40 ? 'bg-amber-500' : 'bg-blue-500';
                const tr = document.createElement('tr');
                tr.classList.add('hover:bg-slate-800');
                tr.innerHTML = `
                    <td class="px-4 py-3 font-medium text-white">${reg.name}</td>
                    <td class="px-4 py-3">
                        <div class="flex items-center gap-3">
                            <span class="text-xs font-bold w-10 text-right">${reg.progress}%</span>
                            <div class="flex-1 bg-slate-700 rounded-full h-2.5">
                                <div class="${color} h-full rounded-full" style="width: ${reg.progress}%"></div>
                            </div>
                        </div>
                    </td>
                    <td class="px-4 py-3 text-center text-blue-300 font-bold">${reg.type_mod}</td>
                    <td class="px-4 py-3 text-center text-purple-300 font-bold">${reg.type_new}</td>
                    <td class="px-4 py-3 text-right text-green-400 font-bold">${reg.done_mod}</td>
                    <td class="px-4 py-3 text-right text-emerald-400 font-bold">${reg.done_new}</td>
                    <td class="px-4 py-3 text-right text-yellow-400">${reg.status_inp}</td>
                    <td class="px-4 py-3 text-right text-cyan-400">${reg.status_sms}</td>
                    <td class="px-4 py-3 text-right text-slate-400">${reg.status_new}</td>
                    <td class="px-4 py-3 text-right font-bold text-white">${reg.total}</td>
                `;
                tbody.appendChild(tr);
            });
        }

        // --- GRAPH FUNCTIONS ---

        function showGraphModal() {
            toggleGraphView();
        }

        function toggleGraphView() {
            const statsPanel = document.getElementById('stats-panel');
            const graphPanel = document.getElementById('graph-side-panel');

            if (!isGraphOpen) {
                // Open Graph
                graphPanel.classList.remove('hidden');
                graphPanel.style.display = 'flex';

                // Use a small timeout to allow the display change to register before transitioning props
                setTimeout(() => {
                   statsPanel.classList.remove('w-full');
                   statsPanel.classList.add('w-2/3');
                }, 10);

                isGraphOpen = true;
                loadGraphData();
            } else {
                // Close Graph
                statsPanel.classList.remove('w-2/3');
                statsPanel.classList.add('w-full');

                // Wait for transition to finish before hiding display
                setTimeout(() => {
                    graphPanel.style.display = 'none';
                    graphPanel.classList.add('hidden');
                }, 300);

                isGraphOpen = false;
            }
        }

        function loadGraphData() {
            const periodInput = document.getElementById('graph-period');
            const period = periodInput ? periodInput.value : 'day';

            fetch(`/api/graph_data?period=${period}`)
            .then(r => r.json())
            .then(data => {
                if(data.error) {
                    console.error("Graph Error:", data.error);
                    return;
                }
                renderChart(data);
            })
            .catch(e => console.error("Error loading graph:", e));
        }

        function renderChart(data) {
            const canvas = document.getElementById('progressChart');
            if(!canvas) return;
            const ctx = canvas.getContext('2d');

            if(myChart) myChart.destroy();

            myChart = new Chart(ctx, {
                type: 'line',
                data: { labels: data.labels, datasets: data.datasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { labels: { color: 'white', boxWidth: 10, font: {size: 10} } },
                        title: { display: true, text: 'Completed Bases', color: 'white', font: {size: 14} }
                    },
                    scales: {
                        x: { ticks: { color: '#94a3b8', font: {size: 10} }, grid: { color: '#334155' } },
                        y: { ticks: { color: '#94a3b8', font: {size: 10} }, grid: { color: '#334155' }, beginAtZero: true }
                    }
                }
            });
        }

        // Initialize
        document.addEventListener('DOMContentLoaded', () => {
             fetchData();
        });
    </script>
</body>
</html>
"""

STEP_LABELS = {i: label for i, label in enumerate(
    ["Принятие", "Данные", "Конфиг", "Фото", "Тесты", "Датчики", "Азимут", "Оптим", "Сервис", "SMS"], 1)}

# --- [БЛОК 3: ХРАНИЛИЩЕ ДАННЫХ] ---
data_store = {
    "last_sync_dt": None,
    "combined_sms": "Данные еще не загружены...",
    "zte": {
        "prog_table": [],
        "region_stats": {},
        "stats": {},
        "pending": {i: 0 for i in range(1, 11)}
    },
    "huawei": {
        "prog_table": [],
        "region_stats": {},
        "stats": {},
        "pending": {i: 0 for i in range(1, 11)}
    },
    "active_alarms": {},
    "alarm_errors": [],
    "id_bridge": {},
    "sms_numeric_ids": {"zte": set(), "huawei": set()},
    "full_export_data": []
}


# --- [БЛОК 4: ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ] ---


def get_num(text):
    """ Очистка ID для сопоставления с алармами """
    if not text:
        return ""

    text_str = str(text)

    # Для Huawei: извлечь число из скобок, например "Beshquorgon Toshkent(5162)" -> "5162"
    match = re.search(r'\((\d+)\)', text_str)
    if match:
        return match.group(1)

    # Для остальных: извлечь все цифры
    clean_id = text_str.split('_')[0]
    return "".join(re.findall(r'\d+', clean_id))


def parse_site_info(text):
    """ Парсинг ID из скобок (1234) """
    text = str(text)
    id_match = re.search(r'\((\d+)\)', text)
    sid = id_match.group(1) if id_match else ""
    return sid, text


def generate_sms_text(df_sms_input):
    """ Умное формирование СМС (Запуск/Модернизация) """
    if df_sms_input.empty:
        return "Ассалому алейкум, сегодня новых объектов для SMS нет."

    # Создаем копию для предотвращения SettingWithCopyWarning
    df_sms = df_sms_input.copy()
    today = datetime.now().strftime("%d.%m.%Yг")
    obj_type_col = 'Тип объекта' if 'Тип объекта' in df_sms.columns else 'Type'

    # Универсальный поиск колонки ID
    id_col = next((c for c in ['ID БТС', 'Код БТС', 'ID'] if c in df_sms.columns), None)

    # Сортировка по Региону
    df_sms['RegSort'] = df_sms['Регион'].map(lambda x: REGIONS_MAP.get(str(x).strip(), str(x)))
    df_sms = df_sms.sort_values(by='RegSort')

    df_new = df_sms[df_sms[obj_type_col].astype(str).str.lower() == 'new']
    df_mod = df_sms[df_sms[obj_type_col].astype(str).str.lower() != 'new']

    # Формируем динамическую шапку
    header_parts = []
    if not df_new.empty:
        word_new = "запущена" if len(df_new) == 1 else "запущены"
        header_parts.append(f"{word_new} {len(df_new)} БС")
    if not df_mod.empty:
        word_mod = "модернизирована" if len(df_mod) == 1 else "модернизированы"
        header_parts.append(f"{word_mod} {len(df_mod)} БС")

    header_str = " и ".join(header_parts)
    full_text = f"Ассалому Алaйкум, сегодня в зоне Huawei и ZTE {header_str} ({today})\n"

    def clean_t(val):
        t = str(val).replace(', ', '\\').replace('/', '\\').strip()
        return t if t and t not in ['nan', 'None', '--', '-', 'данные отсутствуют'] else ""

    # Секция ЗАПУСК
    if not df_new.empty:
        full_text += "\nЗапуск\n"
        for i, (_, row) in enumerate(df_new.iterrows(), 1):
            nm = str(row.get('Название', '')).split(')')[-1].strip()
            id_b = str(row.get(id_col, '')).split('_')[0].strip() if id_col else "ID не указан"
            if not id_b or id_b == 'nan': id_b = "ID не указан"

            rg = REGIONS_MAP.get(str(row.get('Регион', '')).strip(), str(row.get('Регион', '')))
            t_a = clean_t(row.get('Технологии После', '--'))
            full_text += f"{i}. Запустили новую БС {nm} ({id_b}) {t_a} в {rg}\n"

    # Секция МОДЕРНИЗАЦИЯ
    if not df_mod.empty:
        full_text += "\nМодернизация\n"
        for i, (_, row) in enumerate(df_mod.iterrows(), 1):
            nm = str(row.get('Название', '')).split(')')[-1].strip()
            id_b = str(row.get(id_col, '')).split('_')[0].strip() if id_col else "ID не указан"
            if not id_b or id_b == 'nan': id_b = "ID не указан"

            rg = REGIONS_MAP.get(str(row.get('Регион', '')).strip(), str(row.get('Регион', '')))
            t_b = clean_t(row.get('Технологии До', '--'))
            t_a = clean_t(row.get('Технологии После', '--'))

            tech_str = f"с {t_b} на {t_a}" if t_b and t_a else (t_a if t_a else "")
            full_text += f"{i}. Произведена модернизация на БС {nm} ({id_b}) {tech_str} в {rg}\n"

    return full_text


# --- [БЛОК 5: СИНХРОНИЗАЦИЯ ДАННЫХ] ---

def process_session_data(session):
    """ Скачивание и обработка файла с портала """
    try:
        print(flush=True, ">>> [SYNC] Скачивание файла...")
        res = session.get(f"{BASE_URL}/Bts/ExportAuditToExcel", timeout=120, verify=False)

        if b"<!DOCTYPE html>" in res.content[:100]:
            print(flush=True, ">>> [ERROR] Вместо Excel пришел HTML. Сессия сброшена.")
            if os.path.exists(COOKIE_FILE): os.remove(COOKIE_FILE)
            return False

        df_audit = pd.read_excel(BytesIO(res.content), engine='openpyxl')
        df_audit.columns = [str(col).strip() for col in df_audit.columns]

        df_full = df_audit[df_audit['Статус'].isin(['New', 'InProgress', 'SMS', 'Done'])].copy()

        # Динамический поиск колонок шагов
        step_cols = {}
        for i in range(1, 11):
            found = [c for c in df_audit.columns if f"Шаг {i}:" in c and "Статус" in c]
            step_cols[i] = found[0] if found else None

        vendor_col = 'Вендор' if 'Вендор' in df_full.columns else 'Vendor'
        obj_type_col = 'Тип объекта' if 'Тип объекта' in df_full.columns else 'Type'

        sms_frames = []
        data_store["sms_numeric_ids"] = {"zte": set(), "huawei": set()}
        data_store["id_bridge"] = {}
        data_store["full_export_data"] = []

        for vk in ['huawei', 'zte']:
            data_store[vk] = {"prog_table": [], "region_stats": {}, "pending": {i: 0 for i in range(1, 11)},
                              "stats": {}}

            v_df = df_full[df_full[vendor_col].astype(str).str.upper().str.contains(vk.upper())]
            v_ui_df = v_df[v_df['Статус'] != 'Done']

            # Use step 1 header if available, else fallback
            s1_col = step_cols.get(1)
            if s1_col:
                v_active_ui = v_ui_df[v_ui_df[s1_col].astype(str).str.strip() == 'Завершено'].copy()
            else:
                v_active_ui = v_ui_df.copy()

            v_sms_today = v_active_ui[v_active_ui['Статус'] == 'SMS']
            sms_frames.append(v_sms_today)

            ui_list = []
            wait_t_new, wait_t_mod = [], []
            wait_s_new, wait_s_mod = [], []
            wait_c_new, wait_c_mod = [], []  # Config waiting
            wait_srv_new, wait_srv_mod = [], []  # Service waiting

            for _, row in v_active_ui.iterrows():
                f_id = str(row.get('Код БТС', '')).split('_')[0].strip()
                num_id = get_num(f_id)
                o_type = str(row.get(obj_type_col, 'mod')).lower()

                data_store["id_bridge"][num_id] = f_id
                if str(row.get('Статус')) == 'SMS':
                    data_store["sms_numeric_ids"][vk].add(num_id)

                entry = {
                    'ID': f_id,
                    'NumID': num_id,
                    'Reg': REGIONS_MAP.get(str(row.get('Регион', '')).strip(), str(row.get('Регион', ''))),
                    'Name': str(row.get('Название', '')).split(')')[-1].strip(),
                    'ObjType': o_type,
                    'PortalStatus': str(row.get('Статус', 'InProgress'))
                }

                done_c = 0
                for i in range(1, 11):
                    sc = step_cols.get(i)
                    val = str(row[sc]).strip() if sc else "---"
                    entry[f'S{i}'] = val
                    if val == 'Завершено':
                        data_store[vk]['pending'][i] += 1
                        done_c += 1

                    # Step 3 - Config: track if NOT completed and previous step IS completed
                    if i == 3:
                        step2_col = step_cols.get(2)
                        step2_val = str(row[step2_col]).strip() if step2_col else ""
                        # If Step 2 is done but Step 3 is not done
                        if step2_val == 'Завершено' and val != 'Завершено':
                            if o_type == 'new':
                                wait_c_new.append(f_id)
                            else:
                                wait_c_mod.append(f_id)

                    # Step 5 - Tests waiting
                    if i == 5 and val == 'Ожидает проверки':
                        if o_type == 'new':
                            wait_t_new.append(f_id)
                        else:
                            wait_t_mod.append(f_id)

                    # Step 6 - Sensors waiting
                    if i == 6 and val == 'Ожидает проверки':
                        if o_type == 'new':
                            wait_s_new.append(f_id)
                        else:
                            wait_s_mod.append(f_id)

                    # Step 9 - Service: track if NOT completed and previous step IS completed
                    if i == 9:
                        step8_col = step_cols.get(8)
                        step8_val = str(row[step8_col]).strip() if step8_col else ""
                        # If Step 8 is done but Step 9 is not done
                        if step8_val == 'Завершено' and val != 'Завершено':
                            if o_type == 'new':
                                wait_srv_new.append(f_id)
                            else:
                                wait_srv_mod.append(f_id)

                entry['Pct'] = int((done_c / 10) * 100)
                ui_list.append(entry)

            # Export Logic
            # Export Logic - Include ALL sites, don't filter by Step 1 completion
            v_export_df = v_df.copy()

            for _, row in v_export_df.iterrows():
                date_val = '-'
                s10_col = step_cols.get(10)
                if s10_col:
                    # Try to find date column for step 10
                    # Usually named like "Шаг 10: ... - Дата выполнения"
                    s10_date_col = next((c for c in df_audit.columns if "Шаг 10" in c and "Дата выполнения" in c), None)
                    if s10_date_col:
                        raw_date = row.get(s10_date_col)
                        if pd.notna(raw_date):
                            date_val = str(raw_date)

                ex_entry = {
                    'Вендор': vk.upper(),
                    'ID БТС': str(row.get('Код БТС', '')).split('_')[0].strip(),
                    'Регион': REGIONS_MAP.get(str(row.get('Регион', '')).strip(), str(row.get('Регион', ''))),
                    'Название': str(row.get('Название', '')).split(')')[-1].strip(),
                    'Тип объекта': str(row.get(obj_type_col, 'mod')),
                    'Статус': str(row.get('Статус', '---')),
                    'Технологии До': row.get('Технологии До', ''),
                    'Технологии После': row.get('Технологии После', ''),
                    'CompletionDate': date_val
                }
                st_d = 0
                for i in range(1, 11):
                    sc = step_cols.get(i)
                    sv = str(row[sc]).strip() if sc else "-"
                    ex_entry[f'Шаг {i} ({STEP_LABELS[i]})'] = sv
                    if sv == 'Завершено': st_d += 1
                ex_entry['%'] = int((st_d / 10) * 100)
                data_store["full_export_data"].append(ex_entry)

            data_store[vk]['pending'][10] = len(v_sms_today)
            data_store[vk]['prog_table'] = sorted(ui_list, key=lambda x: x['Pct'], reverse=True)
            data_store[vk]['region_stats'] = {
                REGIONS_MAP.get(k.strip(), k): v for k, v in v_active_ui.groupby('Регион').size().to_dict().items()
            }

            data_store[vk]['stats'] = {
                'total_new': len(v_active_ui[v_active_ui[obj_type_col].astype(str).str.lower() == 'new']),
                'total_mod': len(v_active_ui[v_active_ui[obj_type_col].astype(str).str.lower() != 'new']),
                'sms_new': len(v_sms_today[v_sms_today[obj_type_col].astype(str).str.lower() == 'new']),
                'sms_mod': len(v_sms_today[v_sms_today[obj_type_col].astype(str).str.lower() != 'new']),
                'c_new': len(wait_c_new), 'c_mod': len(wait_c_mod),
                't_new': len(wait_t_new), 't_mod': len(wait_t_mod),
                's_new': len(wait_s_new), 's_mod': len(wait_s_mod),
                'srv_new': len(wait_srv_new), 'srv_mod': len(wait_srv_mod),
                'c_ids': ", ".join(wait_c_new + wait_c_mod) or "Нет",
                't_ids': ", ".join(wait_t_new + wait_t_mod) or "Нет",
                's_ids': ", ".join(wait_s_new + wait_s_mod) or "Нет",
                'srv_ids': ", ".join(wait_srv_new + wait_srv_mod) or "Нет"
            }

        data_store['combined_sms'] = generate_sms_text(pd.concat(sms_frames) if sms_frames else pd.DataFrame())
        data_store['last_sync_dt'] = datetime.now()
        print(flush=True, ">>> [SYNC] Данные успешно обновлены.")
        return True

    except:
        traceback.print_exc()
        return False


def fetch_and_sync_all():
    try:
        if not os.path.exists(CONFIG_FILE):
            print(flush=True, ">>> [ERROR] user_config.json не найден")
            return False
        with open(CONFIG_FILE, "r") as f:
            creds = json.load(f)

        session = requests.Session()

        # 1. Cookies Login
        if os.path.exists(COOKIE_FILE):
            try:
                with open(COOKIE_FILE, 'rb') as f:
                    session.cookies.update(pickle.load(f))
                test_res = session.get(f"{BASE_URL}/Bts", verify=False, timeout=15)
                if test_res.status_code == 200 and "/Account/Login" not in test_res.url:
                    print(flush=True, ">>> [SESSION] COOKIE VALID.")
                    return process_session_data(session)
            except:
                pass

        # 2. Login
        print(flush=True, ">>> [LOGIN] Пробуем вход по логину/паролю...")
        resp = session.get(f"{BASE_URL}/Account/Login", verify=False)
        token_input = BeautifulSoup(resp.content, 'html.parser').find('input', {'name': '__RequestVerificationToken'})
        if not token_input:
            print(flush=True, ">>> [ERROR] Token not found")
            return False

        token_l = token_input['value']
        print(flush=True, f">>> [DEBUG] Логин: {creds['u']}, Token: {token_l[:30]}...")

        login_data = {'LoginInput': creds['u'], 'Password': creds['p'], '__RequestVerificationToken': token_l}
        res_l = session.post(f"{BASE_URL}/Account/Login", data=login_data, verify=False, allow_redirects=True)

        print(flush=True, f">>> [DEBUG] После логина: status={res_l.status_code}, url={res_l.url}")

        # 3. 2FA Check
        if "LoginWith2fa" in res_l.url:
            print(flush=True, f">>> [SMS] Введите код из SMS, отправленный на ***{creds['u'][-4:]}:")
            sms_code = input().strip()

            print(flush=True, f">>> [DEBUG] Введенный код: '{sms_code}', длина: {len(sms_code)}")

            # Validate code length
            if len(sms_code) != 6 or not sms_code.isdigit():
                print(flush=True, ">>> [ERROR] Код должен содержать ровно 6 цифр!")
                return False

            # Parse verification token from 2FA page
            soup_2fa = BeautifulSoup(res_l.content, 'html.parser')
            token_2fa = soup_2fa.find('input', {'name': '__RequestVerificationToken'})
            ver_token = token_2fa['value'] if token_2fa else ""

            print(flush=True, f">>> [DEBUG] 2FA Token: {ver_token[:30]}...")

            # Submit 2FA code - server expects D1, D2, D3, D4, D5, D6 fields
            submit_data = {
                "D1": sms_code[0],
                "D2": sms_code[1],
                "D3": sms_code[2],
                "D4": sms_code[3],
                "D5": sms_code[4],
                "D6": sms_code[5],
                "__RequestVerificationToken": ver_token
            }
            print(flush=True, f">>> [DEBUG] Отправляем 2FA на: {BASE_URL}/Account/LoginWith2fa")
            print(flush=True, 
                f">>> [DEBUG] Данные: D1={submit_data['D1']}, D2={submit_data['D2']}, D3={submit_data['D3']}, D4={submit_data['D4']}, D5={submit_data['D5']}, D6={submit_data['D6']}")

            res_2fa = session.post(f"{BASE_URL}/Account/LoginWith2fa", data=submit_data, verify=False,
                                   allow_redirects=True)

            print(flush=True, f">>> [DEBUG] Ответ 2FA: status={res_2fa.status_code}, url={res_2fa.url}")

            if "/Account/Login" in res_2fa.url:
                print(flush=True, ">>> [ERROR] Неверный код или истекло время.")
                # Print error message if any
                soup_err = BeautifulSoup(res_2fa.content, 'html.parser')
                err_div = soup_err.find('div', class_='text-danger')
                if err_div:
                    print(flush=True, f">>> [ERROR] Сообщение: {err_div.get_text().strip()}")
                return False

            print(flush=True, ">>> [LOGIN] 2FA успешно. Сохраняем куки.")
            with open(COOKIE_FILE, 'wb') as f:
                pickle.dump(session.cookies, f)
            return process_session_data(session)

        # 4. Success without 2FA?
        if "/Account/Login" in res_l.url:
            print(flush=True, f">>> [ERROR] Не удалось войти. URL: {res_l.url}")
            # Print error message from page
            soup_err = BeautifulSoup(res_l.content, 'html.parser')
            err_div = soup_err.find('div', class_='text-danger')
            if err_div:
                print(flush=True, f">>> [ERROR] Сообщение от сервера: {err_div.get_text().strip()}")
            return False

        if res_l.status_code == 200:
            print(flush=True, ">>> [LOGIN] Вход успешен (200 OK). Сохраняем куки.")
            with open(COOKIE_FILE, 'wb') as f: pickle.dump(session.cookies, f)
            return process_session_data(session)

        print(flush=True, f">>> [ERROR] Странный ответ сервера: {res_l.status_code} {res_l.url}")
        return False

    except Exception as e:
        traceback.print_exc()
        return False


# --- [БЛОК 6: ВЕБ-ИНТЕРФЕЙС] ---

@app.route('/background_image')
def serve_background_image():
    if os.path.exists(BACKGROUND_IMAGE_PATH):
        return send_file(BACKGROUND_IMAGE_PATH, mimetype='image/jpeg')
    else:
        return "Image not found", 404


@app.route('/', methods=['GET', 'POST'])
def index():
    show_modal = False
    data_store['alarm_errors'] = []

    if request.method == 'POST':
        # Don't clear ALL alarms - we'll clear only for uploaded vendors
        # data_store['active_alarms'] = {}  # OLD: This cleared everything!

        try:
            for vk, file_key in [('huawei', 'hw_alarms'), ('zte', 'zte_alarms')]:
                file = request.files.get(file_key)
                if not file or file.filename == '':
                    print(flush=True, f">>> [ALARM CHECK] No file uploaded for {vk.upper()}")
                    continue

                # Clear alarms ONLY for this vendor before processing
                print(flush=True, f">>> [ALARM CHECK] Clearing old alarms for {vk.upper()}...")
                vendor_ids = data_store['sms_numeric_ids'].get(vk, set())
                removed_count = 0
                for num_id in vendor_ids:
                    full_id = data_store['id_bridge'].get(num_id, num_id)
                    if full_id in data_store['active_alarms']:
                        del data_store['active_alarms'][full_id]
                        removed_count += 1
                print(flush=True, f">>> [ALARM CHECK] Cleared {removed_count} alarm entries for {vk.upper()}")

                print(flush=True, f"\n>>> [ALARM CHECK] ========== Processing {vk.upper()} file: {file.filename} ==========")

                # Extract lookup keys to avoid f-string nesting issues
                vendor_prefix = 'hw' if vk == 'huawei' else 'zt'
                id_lookup_key = f"{vendor_prefix}_id"
                msg_lookup_key = f"{vendor_prefix}_msg"

                print(flush=True, f">>> [ALARM CHECK] Lookup keys: {id_lookup_key}, {msg_lookup_key}")
                print(flush=True, f">>> [ALARM CHECK] Expected ID columns: {COLUMN_LOOKUP[id_lookup_key]}")
                print(flush=True, f">>> [ALARM CHECK] Expected MSG columns: {COLUMN_LOOKUP[msg_lookup_key]}")

                if file.filename.lower().endswith('.zip'):
                    print(flush=True, f">>> [ALARM CHECK] Processing ZIP file...")
                    with zipfile.ZipFile(file) as z:
                        excel_files = [f for f in z.namelist() if f.lower().endswith(('.xlsx', '.xls'))]
                        if not excel_files:
                            print(flush=True, f">>> [ALARM CHECK] No Excel files in ZIP!")
                            continue
                        print(flush=True, f">>> [ALARM CHECK] Found Excel in ZIP: {excel_files[0]}")
                        with z.open(excel_files[0]) as f_inside:
                            excel_content = f_inside.read()
                else:
                    print(flush=True, f">>> [ALARM CHECK] Processing Excel file directly...")
                    excel_content = file.read()

                # Step 1: Read raw file WITHOUT headers (like FTP Process)
                print(flush=True, f">>> [ALARM CHECK] Reading raw file...")
                df_raw = pd.read_excel(BytesIO(excel_content), header=None, engine='openpyxl')
                print(flush=True, f">>> [ALARM CHECK] Raw file loaded: {len(df_raw)} rows, {len(df_raw.columns)} columns")

                # Show first 5 rows of raw file to inspect structure
                print(flush=True, f">>> [ALARM CHECK] First 5 rows of raw file:")
                for idx in range(min(5, len(df_raw))):
                    row_values = df_raw.iloc[idx].values.tolist()
                    # Show only first 5 values to avoid clutter
                    print(flush=True, f"    Row {idx}: {row_values[:5]}")

                # Find header row - SCAN ALL ROWS (like FTP Process)
                lookup_cols = COLUMN_LOOKUP[id_lookup_key]
                print(flush=True, f">>> [ALARM CHECK] Looking for header containing any of: {lookup_cols}")

                h_idx = -1
                for i, row in df_raw.iterrows():
                    if any(str(v) in lookup_cols for v in row.values):
                        h_idx = i
                        break

                if h_idx == -1:
                    print(flush=True, f">>> [ALARM CHECK] ✗ ERROR: Header not found for {vk.upper()}!")
                    print(flush=True, f">>> [ALARM CHECK] File may not be a valid {vk.upper()} alarm export")
                    continue

                print(flush=True, f">>> [ALARM CHECK] Vendor: {vk.upper()}, Header index found: {h_idx}")
                print(flush=True, f">>> [ALARM CHECK] Header row content: {df_raw.iloc[h_idx].values.tolist()[:10]}")

                # Step 2: Re-read with correct header (like FTP Process)
                print(flush=True, f">>> [ALARM CHECK] Re-reading file with proper headers...")
                df = pd.read_excel(BytesIO(excel_content), skiprows=h_idx, engine='openpyxl')
                df.columns = [str(c).strip() for c in df.columns]

                print(flush=True, f">>> [ALARM CHECK] Data loaded: {len(df)} rows")
                print(flush=True, f">>> [ALARM CHECK] Columns: {list(df.columns)[:10]}")

                id_col_al = next(
                    (c for c in COLUMN_LOOKUP[id_lookup_key] if c in df.columns), None)
                msg_col_al = next(
                    (c for c in COLUMN_LOOKUP[msg_lookup_key] if c in df.columns), None)

                print(flush=True, f">>> [ALARM CHECK] Matched ID column: '{id_col_al}'")
                print(flush=True, f">>> [ALARM CHECK] Matched MSG column: '{msg_col_al}'")

                if id_col_al and msg_col_al:
                    alarm_count = 0
                    processed_count = 0

                    # Show first 3 rows for debugging
                    print(flush=True, f">>> [ALARM CHECK] Sample data from {vk.upper()} file (first 3 rows):")
                    for idx, r in df.head(3).iterrows():
                        raw_id = r.get(id_col_al, "")
                        msg = r.get(msg_col_al, "")
                        print(flush=True, f"    Row {idx}: ID='{raw_id}', MSG='{msg}'")

                    # Check what's in the database for this vendor
                    print(flush=True, 
                        f">>> [ALARM CHECK] SMS-ready sites in DB for {vk.upper()}: {len(data_store['sms_numeric_ids'][vk])}")
                    if len(data_store['sms_numeric_ids'][vk]) > 0:
                        sample_ids = list(data_store['sms_numeric_ids'][vk])[:5]
                        print(flush=True, f">>> [ALARM CHECK] Sample IDs from DB: {sample_ids}")
                    else:
                        print(flush=True, f">>> [ALARM CHECK] WARNING: No SMS-ready sites in database for {vk.upper()}!")

                    for _, r in df.iterrows():
                        processed_count += 1
                        raw_id_val = r.get(id_col_al, "")
                        sid_n, _ = parse_site_info(raw_id_val)
                        if not sid_n:
                            sid_n = get_num(raw_id_val)

                        # Debug first 5 matches
                        if processed_count <= 5:
                            in_db = sid_n in data_store['sms_numeric_ids'][vk]
                            print(flush=True, 
                                f"    Processing row {processed_count}: '{raw_id_val}' -> extracted: '{sid_n}' -> in DB: {in_db}")

                        if sid_n in data_store["sms_numeric_ids"][vk]:
                            f_id = data_store["id_bridge"].get(sid_n, sid_n)
                            msg = str(r.get(msg_col_al, "---"))
                            data_store['active_alarms'].setdefault(f_id, []).append(msg)
                            alarm_count += 1

                    print(flush=True, 
                        f">>> [ALARM CHECK] ✓ Processed {processed_count} rows, found {alarm_count} matching alarms for {vk.upper()}")
                    print(flush=True, 
                        f">>> [ALARM CHECK] Total sites in DB for {vk.upper()}: {len(data_store['sms_numeric_ids'][vk])}")
                else:
                    print(flush=True, f">>> [ALARM CHECK] ✗ ERROR: Missing columns for {vk.upper()}!")
                    print(flush=True, f">>> [ALARM CHECK] Available columns in file: {list(df.columns)}")

            # Format only NEW alarms (lists), don't re-format already formatted strings
            for k, v in data_store['active_alarms'].items():
                if isinstance(v, list):  # Only format if it's still a list
                    data_store['active_alarms'][k] = "<br> • " + "<br> • ".join(list(set(v)))

            show_modal = True
        except:
            traceback.print_exc()
            show_modal = True

    lu = data_store['last_sync_dt'].strftime('%H:%M:%S') if data_store['last_sync_dt'] else "---"

    def prepare(vk):
        if vk not in data_store or not data_store[vk]:
            return {
                "regs": "",
                "counts": "".join(
                    [f'<td class="count-cell"><div class="count-num">0</div></td>' for _ in range(1, 11)]),
                "rows": "<tr><td colspan='20'>Данные не загружены. Нажмите ОБНОВИТЬ.</td></tr>",
                "stats": {"total_new": 0, "total_mod": 0, "c_new": 0, "c_mod": 0, "t_new": 0, "t_mod": 0,
                          "s_new": 0, "s_mod": 0, "srv_new": 0, "srv_mod": 0, "sms_new": 0, "sms_mod": 0,
                          "c_ids": "", "t_ids": "", "s_ids": "", "srv_ids": ""}
            }

        vd = data_store[vk]

        sort_order = {"SMS": 0, "InProgress": 1, "New": 2, "Done": 3}
        sorted_table = sorted(vd.get('prog_table', []), key=lambda x: sort_order.get(x['PortalStatus'], 99))

        rows = ""
        for i, item in enumerate(sorted_table):
            al_text = data_store.get('active_alarms', {}).get(item['ID'], "")
            al = f"<br><span style='color:red; font-size:10px; line-height:1.2;'>{al_text}</span>" if al_text else ""
            obj_style = "background:#3182ce; color:white; padding:2px 8px; border-radius:12px; font-weight:bold; font-size:10px;" if \
                item[
                    'ObjType'] == 'new' else "background:#edf2f7; color:#4a5568; padding:2px 8px; border-radius:12px; font-size:10px;"
            is_sms_ready = item.get('PortalStatus') == 'SMS'
            selector = f'<input type="checkbox" class="site-checkbox" value="{item["ID"]}" style="width:18px; height:18px; cursor:pointer;">' if is_sms_ready else f'<span style="color:#ccc;">{i + 1}</span>'

            rows += f"<tr><td>{selector}</td><td>{item['Reg']}</td><td><b>{item['ID']}</b></td>" \
                    f"<td style='text-align:left;'>{item['Name']}{al}</td>" \
                    f"<td><span style='{obj_style}'>{item['ObjType']}</span></td>" \
                    f"<td><span style='color:{'#38a169' if is_sms_ready else '#718096'}; font-weight:bold;'>{item['PortalStatus']}</span></td>" + "".join(
                [
                    f'<td style="background:{"#4CAF50" if item.get(f"S{s}") == "Завершено" else "#eee"}; border:1px solid #ddd; height:12px;"></td>'
                    for s in range(1, 11)]) + \
                    f"<td><b>{item['Pct']}%</b></td></tr>"

        return {
            "regs": "".join([f'<div class="reg-card"><b>{r}</b><span>{c}</span></div>' for r, c in
                             sorted(vd.get('region_stats', {}).items())]),
            "counts": "".join([
                f'<td class="count-cell"><div class="count-num">{vd["pending"][i] if i > 1 else (vd["stats"].get("total_new", 0) + vd["stats"].get("total_mod", 0))}</div></td>'
                for i in range(1, 11)]),
            "rows": rows,
            "stats": vd.get('stats', {})
        }

    z_v, h_v = prepare('zte'), prepare('huawei')
    steps_header = "".join([f'<th style="font-size: 10px; width: 55px;">{STEP_LABELS[i]}</th>' for i in range(1, 11)])

    return render_template_string('''
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>BTS MONITOR v30.0</title>
            <link rel="preconnect" href="https://fonts.googleapis.com">
            <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
            <link href="https://fonts.googleapis.com/css2?family=Lora:ital,wght@0,400;0,600;1,400&family=Outfit:wght@400;600;700;900&family=Poppins:wght@400;500;600;700&display=swap" rel="stylesheet">
            <style>
                :root {
                    --bg-color: #faf9f5;
                    --text-dark: #141413;
                    --text-light: #faf9f5;
                    --gray-mid: #b0aea5;
                    --gray-light: #e8e6dc;
                    --accent-orange: #d97757;
                    --accent-blue: #6a9bcc;
                    --accent-green: #788c5d;
                    --card-bg: rgba(255, 255, 255, 0.85);
                }

                [data-theme="dark"] {
                    --bg-color: #1a1a1a;
                    --text-dark: #e8e6dc;
                    --text-light: #141413;
                    --gray-mid: #6a6a6a;
                    --gray-light: #2a2a2a;
                    --accent-orange: #ff9966;
                    --accent-blue: #88b3dd;
                    --accent-green: #9db87c;
                    --card-bg: rgba(36, 36, 36, 0.85);
                }

                /* Light Orange Theme */
                [data-theme="orange"] {
                    --bg-color: #fff7ed;
                    --text-dark: #7c2d12;
                    --text-light: #fff7ed;
                    --gray-mid: #fdba74;
                    --gray-light: #ffedd5;
                    --accent-orange: #ea580c;
                    --accent-blue: #3b82f6;
                    --accent-green: #22c55e;
                    --card-bg: rgba(255, 255, 255, 0.85);
                }

                /* Light Pink Theme */
                [data-theme="pink"] {
                    --bg-color: #fdf2f8;
                    --text-dark: #831843;
                    --text-light: #fdf2f8;
                    --gray-mid: #f9a8d4;
                    --gray-light: #fce7f3;
                    --accent-orange: #db2777;
                    --accent-blue: #3b82f6;
                    --accent-green: #22c55e;
                    --card-bg: rgba(255, 255, 255, 0.85);
                }

                /* Light Blue Theme */
                [data-theme="blue"] {
                    --bg-color: #eff6ff;
                    --text-dark: #1e3a8a;
                    --text-light: #eff6ff;
                    --gray-mid: #93c5fd;
                    --gray-light: #dbeafe;
                    --accent-orange: #2563eb;
                    --accent-blue: #3b82f6;
                    --accent-green: #22c55e;
                    --card-bg: rgba(255, 255, 255, 0.70);
                }

                /* ===== Glaido Dark Theme ===== */
                [data-theme="glaido"] {
                    --bg-color: #0a0a0a;
                    --text-dark: #f0f0f0;
                    --text-light: #0a0a0a;
                    --gray-mid: #2a2a2a;
                    --gray-light: #161616;
                    --accent-orange: #C7F280;
                    --accent-blue: #C7F280;
                    --accent-green: #C7F280;
                    --card-bg: rgba(12, 12, 12, 0.96);
                }

                /* Glaido: font overrides */
                [data-theme="glaido"] body,
                [data-theme="glaido"] h1,
                [data-theme="glaido"] h2,
                [data-theme="glaido"] h3,
                [data-theme="glaido"] .btn,
                [data-theme="glaido"] th,
                [data-theme="glaido"] td,
                [data-theme="glaido"] .stats-bar,
                [data-theme="glaido"] .reg-card {
                    font-family: 'Outfit', sans-serif;
                }

                /* Glaido: body background pure black */
                [data-theme="glaido"] body {
                    background: #000 !important;
                }

                /* Glaido: card - sharp bento style */
                [data-theme="glaido"] .card {
                    border: 1px solid #1f1f1f;
                    box-shadow: 0 0 0 1px #111, 0 24px 48px rgba(0,0,0,0.8);
                    border-radius: 24px;
                    backdrop-filter: blur(24px);
                }

                /* Glaido: theme controls */
                [data-theme="glaido"] .theme-controls {
                    background: #111;
                    border-color: #222;
                }
                [data-theme="glaido"] .theme-btn:hover { background: #1f1f1f; }
                [data-theme="glaido"] .theme-btn.active { background: #C7F280; color: #000; }

                /* Glaido: buttons */
                [data-theme="glaido"] .btn-gray {
                    background: #111;
                    color: #d0d0d0;
                    border: 1px solid #222;
                }
                [data-theme="glaido"] .btn-gray:hover {
                    border-color: #C7F280;
                    color: #C7F280;
                    box-shadow: 0 0 14px rgba(199,242,128,0.2);
                    filter: none;
                }
                [data-theme="glaido"] .btn-active {
                    background: #C7F280 !important;
                    color: #0a0a0a !important;
                    border: none !important;
                    box-shadow: 0 0 20px rgba(199,242,128,0.35) !important;
                }

                /* Glaido: stats bar */
                [data-theme="glaido"] .stats-bar {
                    background: #0f0f0f;
                    border: 1px solid #1f1f1f;
                }
                [data-theme="glaido"] .new-val { color: #C7F280; }

                /* Glaido: region cards - bento style */
                [data-theme="glaido"] .reg-card {
                    background: #0f0f0f;
                    border: 1px solid #1e1e1e;
                }
                [data-theme="glaido"] .reg-card span {
                    background: #C7F280;
                    color: #000;
                    font-weight: 700;
                }

                /* Glaido: table */
                [data-theme="glaido"] th {
                    background: #0f0f0f;
                    border-bottom-color: #222;
                    letter-spacing: 0.5px;
                }
                [data-theme="glaido"] td { border-bottom-color: #161616; }
                [data-theme="glaido"] tr:hover td { background: #0f0f0f; }
                [data-theme="glaido"] .count-num { color: #C7F280; font-family: 'Outfit', sans-serif; font-weight: 900; }
                [data-theme="glaido"] thead tr:first-child td {
                    background: #000;
                    border-bottom-color: #222;
                }

                /* Glaido: v-badge */
                [data-theme="glaido"] #v-badge {
                    background: #C7F280 !important;
                    color: #000 !important;
                    font-family: 'Outfit', sans-serif;
                    font-weight: 700;
                }

                /* Glaido: h2 title */
                [data-theme="glaido"] h2 {
                    font-family: 'Outfit', sans-serif;
                    font-weight: 900;
                    letter-spacing: -1px;
                    font-size: 1.6rem;
                }

                /* Glaido: waiting panels */
                [data-theme="glaido"] .v-con > div[style*="flex"] > div {
                    background: #0f0f0f !important;
                    border-color: #1e1e1e !important;
                }

                /* Glaido: scrollbar */
                [data-theme="glaido"] ::-webkit-scrollbar { width: 6px; }
                [data-theme="glaido"] ::-webkit-scrollbar-track { background: #000; }
                [data-theme="glaido"] ::-webkit-scrollbar-thumb { background: #333; border-radius: 3px; }
                [data-theme="glaido"] ::-webkit-scrollbar-thumb:hover { background: #C7F280; }

                body { 
                    font-family: 'Lora', serif; 
                    background: url('/background_image') no-repeat center center fixed; 
                    background-size: cover;
                    color: var(--text-dark);
                    padding: 40px; 
                    margin: 0;
                    transition: color 0.3s ease;
                }

                h1, h2, h3, h4, h5, h6, .btn, th, .nav-label {
                    font-family: 'Poppins', sans-serif;
                }

                .card { 
                    max-width: 1600px; 
                    margin: auto; 
                    background: var(--card-bg); 
                    border-radius: 16px; 
                    border: 1px solid var(--gray-light);
                    box-shadow: 0 4px 20px rgba(20, 20, 19, 0.05); 
                    padding: 40px;
                    transition: background-color 0.3s ease;
                    position: relative;
                    backdrop-filter: blur(10px);
                    -webkit-backdrop-filter: blur(10px);
                }

                .theme-controls {
                    position: absolute;
                    top: 20px;
                    right: 20px;
                    display: flex;
                    gap: 8px;
                    background: var(--card-bg);
                    padding: 5px;
                    border-radius: 30px;
                    box-shadow: 0 4px 12px rgba(0,0,0,0.1);
                    border: 1px solid var(--gray-light);
                    z-index: 1000;
                }

                .theme-btn {
                    width: 32px;
                    height: 32px;
                    border-radius: 50%;
                    border: none;
                    cursor: pointer;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    font-size: 16px;
                    transition: all 0.2s ease;
                    background: transparent;
                }

                .theme-btn:hover {
                    background: var(--gray-light);
                    transform: scale(1.1);
                }

                .theme-btn.active {
                    background: var(--gray-mid);
                    box-shadow: inset 0 2px 4px rgba(0,0,0,0.1);
                    transform: scale(1.0);
                }

                /* Adjust for dark mode visibility */
                [data-theme="dark"] .theme-controls {
                     background: #333;
                     border-color: #555;
                }
                [data-theme="dark"] .theme-btn:hover {
                    background: #444;
                }
                [data-theme="dark"] .theme-btn.active {
                    background: #555;
                }

                h2 {
                    text-align: center;
                    color: var(--text-dark);
                    font-weight: 600;
                    margin-bottom: 30px;
                    letter-spacing: -0.5px;
                }

                #v-badge {
                    background: var(--text-dark);
                    color: var(--text-light);
                    padding: 4px 12px;
                    border-radius: 20px;
                    font-size: 0.6em;
                    vertical-align: middle;
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                    font-weight: 500;
                }

                .stats-bar { 
                    display: none; 
                    justify-content: space-between; 
                    background: var(--text-dark); 
                    color: var(--text-light); 
                    padding: 20px 30px; 
                    border-radius: 12px; 
                    margin-bottom: 25px; 
                    font-family: 'Poppins', sans-serif;
                    font-size: 14px; 
                    box-shadow: 0 4px 12px rgba(0,0,0,0.1);
                }

                .stats-bar span {
                   display: flex;
                   align-items: center;
                   gap: 8px;
                }

                .new-val { 
                    color: var(--accent-blue); 
                    font-weight: 600; 
                }

                .reg-panel { 
                    background: var(--card-bg); 
                    padding: 20px; 
                    border-radius: 12px; 
                    margin-bottom: 25px; 
                    border: 1px solid var(--gray-light);
                }

                .reg-grid { 
                    display: grid; 
                    grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); 
                    gap: 12px; 
                }

                .reg-card { 
                    background: var(--bg-color); 
                    color: var(--text-dark); 
                    padding: 10px 14px; 
                    border-radius: 8px; 
                    display: flex; 
                    justify-content: space-between; 
                    align-items: center;
                    font-family: 'Poppins', sans-serif;
                    font-size: 12px; 
                    border: 1px solid var(--gray-light);
                }

                .reg-card b { font-weight: 600; }
                .reg-card span { 
                    background: var(--text-dark);
                    color: var(--text-light);
                    padding: 2px 6px;
                    border-radius: 4px;
                    font-size: 10px;
                }

                .nav { 
                    display: flex; 
                    gap: 12px; 
                    justify-content: center; 
                    margin-bottom: 30px; 
                    flex-wrap: wrap;
                }

                .nav .btn {
                    width: 170px;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    padding: 12px 10px;
                    box-sizing: border-box;
                    white-space: nowrap;
                    font-size: 13px;
                }

                .btn { 
                    padding: 12px 28px; 
                    border-radius: 8px; 
                    border: none; 
                    color: white; 
                    cursor: pointer; 
                    font-weight: 500; 
                    font-size: 14px;
                    transition: all 0.25s ease; 
                    box-shadow: 0 2px 4px rgba(0,0,0,0.05);
                    position: relative;
                    overflow: hidden;
                }

                .btn:hover { 
                    transform: translateY(-2px); 
                    box-shadow: 0 6px 16px rgba(0,0,0,0.18);
                    filter: brightness(1.08);
                }
                .btn:active { transform: translateY(0); filter: brightness(0.95); }

                /* Ripple animation */
                @keyframes ripple-anim {
                    to { transform: scale(4); opacity: 0; }
                }
                .ripple-circle {
                    position: absolute;
                    border-radius: 50%;
                    background: rgba(255,255,255,0.45);
                    transform: scale(0);
                    animation: ripple-anim 0.55s linear;
                    pointer-events: none;
                }

                /* Brand Colors for Buttons */
                .btn-blue { background: var(--accent-blue); color: var(--text-dark); } 
                .btn-green { background: var(--accent-green); } 
                .btn-red { background: var(--accent-orange); } /* Using Orange for "Red"/Huawei */
                .btn-orange { background: var(--text-dark); border: 1px solid var(--text-dark); } 
                .btn-gray { background: var(--bg-color); color: var(--text-dark); border: 1px solid var(--gray-mid); }

                .btn-active { 
                    background: var(--accent-green) !important;
                    color: white !important;
                    box-shadow: 0 0 0 2px var(--bg-color), 0 0 0 4px var(--accent-green); 
                    transform: translateY(-2px); 
                    font-weight: 700;
                    border: none;
                }

                table { 
                    width: 100%; 
                    border-collapse: separate; 
                    border-spacing: 0;
                    font-size: 12px; 
                    margin-top: 20px;
                } 

                th { 
                    background: var(--gray-light); 
                    color: var(--text-dark); 
                    padding: 12px; 
                    font-weight: 600;
                    text-align: center;
                    border-bottom: 2px solid var(--gray-mid);
                    cursor: pointer;
                    user-select: none;
                    position: sticky;
                    top: 50px;
                    z-index: 90;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.05);
                }

                /* Make the TOTAL row sticky as well, right above the headers */
                thead tr:first-child td {
                    position: sticky;
                    top: 0; 
                    z-index: 100;
                    background: var(--bg-color);
                    box-shadow: 0 4px 4px -2px rgba(0,0,0,0.1);
                    border-bottom: 2px solid var(--gray-mid);
                }

                th:first-child { border-top-left-radius: 8px; }
                th:last-child { border-top-right-radius: 8px; }

                td { 
                    border-bottom: 1px solid var(--gray-light); 
                    padding: 10px; 
                    text-align: center; 
                    vertical-align: middle;
                }

                tr:last-child td { border-bottom: none; }

                .count-num { 
                    color: var(--accent-blue); 
                    font-weight: 700; 
                    font-size: 20px; 
                    font-family: 'Poppins', sans-serif;
                }

                /* Tags */
                .tag-new {
                    background: var(--accent-blue);
                    color: white;
                    padding: 4px 10px;
                    border-radius: 12px;
                    font-weight: 600;
                    font-size: 10px;
                    font-family: 'Poppins', sans-serif;
                    text-transform: uppercase;
                }

                .tag-mod {
                    background: var(--gray-light);
                    color: var(--text-dark);
                    padding: 4px 10px;
                    border-radius: 12px;
                    font-weight: 600;
                    font-size: 10px;
                    font-family: 'Poppins', sans-serif;
                    text-transform: uppercase;
                }

                .status-sms { color: var(--accent-green); font-weight: 700; }
                .status-other { color: var(--gray-mid); font-weight: 500; }

                /* Modal/Panel Styling */
                #check-p > div {
                    box-shadow: 0 10px 40px rgba(0,0,0,0.2);
                    border: 1px solid var(--gray-light);
                }

                input[type="checkbox"] {
                    accent-color: var(--accent-green);
                    width: 16px;
                    height: 16px;
                }
            </style>
            <script>
                // ---- Ripple effect for all .btn ----
                document.addEventListener('click', function(e) {
                    const btn = e.target.closest('.btn');
                    if (!btn) return;
                    const circle = document.createElement('span');
                    const diameter = Math.max(btn.clientWidth, btn.clientHeight);
                    const radius = diameter / 2;
                    const rect = btn.getBoundingClientRect();
                    circle.style.width = circle.style.height = diameter + 'px';
                    circle.style.left = (e.clientX - rect.left - radius) + 'px';
                    circle.style.top  = (e.clientY - rect.top  - radius) + 'px';
                    circle.classList.add('ripple-circle');
                    const old = btn.querySelector('.ripple-circle');
                    if (old) old.remove();
                    btn.appendChild(circle);
                    circle.addEventListener('animationend', () => circle.remove());
                });

                // Theme Toggle
                function setTheme(themeName) {
                    document.documentElement.setAttribute('data-theme', themeName);
                    localStorage.setItem('theme', themeName);
                    updateActiveButton(themeName);
                }

                function updateActiveButton(themeName) {
                    const buttons = document.querySelectorAll('.theme-btn');
                    if (!buttons.length) return;

                    buttons.forEach(btn => {
                        // Extract theme from onclick attribute: setTheme('light')
                        const match = btn.getAttribute('onclick').match(/'([^']+)'/);
                        if (match && match[1] === themeName) {
                            btn.classList.add('active');
                        } else {
                            btn.classList.remove('active');
                        }
                    });
                }

                // Load saved theme on page load
                document.addEventListener('DOMContentLoaded', () => {
                    const savedTheme = localStorage.getItem('theme') || 'light';
                    document.documentElement.setAttribute('data-theme', savedTheme);

                    // We need to wait for the DOM to fully load the buttons if they are not yet available, 
                    // though DOMContentLoaded usually handles it.
                    updateActiveButton(savedTheme);
                });

                function showV(v, btn) {
                    localStorage.setItem('active_vendor', v);

                    document.querySelectorAll('.btn-vendor').forEach(b => b.classList.remove('btn-active'));
                    if(btn) btn.classList.add('btn-active');

                    document.querySelectorAll('.v-con, .stats-bar, #check-p').forEach(el => el.style.display = 'none');

                    const content = document.getElementById('c-' + v);
                    const stats = document.getElementById('s-' + v);

                    // Always show the vendor panels (regions, waiting lists)
                    if(content) content.style.display = 'block';
                    if(stats) stats.style.display = 'flex';

                    // Update Badge
                    const badge = document.getElementById('v-badge');
                    if (v === 'zte') {
                        badge.innerText = 'ACTIVE: ZTE';
                        badge.style.background = 'var(--accent-green)';
                    } else {
                        badge.innerText = 'ACTIVE: HUAWEI';
                        badge.style.background = 'var(--accent-orange)';
                    }
                }

                function toggleMainTable() {
                    const sections = document.querySelectorAll('.table-section');
                    const btn = document.getElementById('main-table-toggle-btn');
                    if (!sections.length) return;
                    const isHidden = sections[0].style.display === 'none' || sections[0].style.display === '';
                    sections.forEach(s => s.style.display = isHidden ? 'block' : 'none');
                    btn.innerHTML = isHidden ? '📋 Hide Main Excel' : '📋 View Main Excel';
                }

                function sortTable(n) {
                    var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
                    table = document.querySelector(".v-con:not([style*='display: none']) table");
                    switching = true; dir = "asc";
                    while (switching) {
                        switching = false; rows = table.rows;
                        for (i = 2; i < (rows.length - 1); i++) {
                            shouldSwitch = false;
                            x = rows[i].getElementsByTagName("TD")[n];
                            y = rows[i + 1].getElementsByTagName("TD")[n];
                            if (dir == "asc") {
                                if (x.innerHTML.toLowerCase() > y.innerHTML.toLowerCase()) { shouldSwitch = true; break; }
                            } else if (dir == "desc") {
                                if (x.innerHTML.toLowerCase() < y.innerHTML.toLowerCase()) { shouldSwitch = true; break; }
                            }
                        }
                        if (shouldSwitch) {
                            rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
                            switching = true; switchcount ++;
                        } else {
                            if (switchcount == 0 && dir == "asc") { dir = "desc"; switching = true; }
                        }
                    }
                }

                function toggleAll(master) {
                    const container = master.closest('.v-con');
                    container.querySelectorAll('.site-checkbox:not(:disabled)').forEach(chk => chk.checked = master.checked);
                }

                function generateSelectedSMS() {
                    const selected = Array.from(document.querySelectorAll('.site-checkbox:checked')).map(cb => cb.value);
                    if (selected.length === 0) return alert("Please select sites first.");
                    fetch('/generate_custom_sms', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({ids: selected})
                    })
                    .then(res => res.json())
                    .then(data => {
                        document.getElementById('t-combined').innerText = data.sms;
                        document.getElementById('sms-panel').style.display = 'block';
                        window.scrollTo(0, 0);
                    });
                }

                function showFullSMS() {
                    const originalText = document.getElementById('sms-storage').innerText;
                    document.getElementById('t-combined').innerText = originalText;
                    document.getElementById('sms-panel').style.display = 'block';
                }

                function testFtp() {
                    const btn = event.srcElement || event.target;
                    const oldText = btn.innerHTML;
                    btn.innerHTML = '⏳ ...';
                    fetch('/ftp_test', {method: 'POST'})
                    .then(r => r.json())
                    .then(d => {
                        alert(d.msg);
                        btn.innerHTML = oldText;
                    })
                    .catch(e => {
                        alert('Error: ' + e);
                        btn.innerHTML = oldText;
                    });
                }

                // Poll for 2FA status
                setInterval(() => {
                    fetch('/check_status')
                    .then(r => r.json())
                    .then(d => {
                        const modal = document.getElementById('sms-2fa-modal');
                        if (d.waiting_sms) {
                            if (modal.style.display === 'none') modal.style.display = 'block';
                        } else {
                             if (modal.style.display === 'block' && !d.waiting_sms) {
                                // optional: modal.style.display = 'none'; 
                             }
                        }
                    });
                }, 2000);

                function downloadFtp() {
                    const btn = event.srcElement || event.target;
                    const oldText = btn.innerHTML;
                    btn.innerHTML = '⏳ ...';
                    fetch('/ftp_process', {method: 'POST'})
                    .then(r => {
                        if (!r.ok) { throw new Error("HTTP " + r.status); }
                        return r.json();
                    })
                    .then(d => {
                        if(d.status === 'ok') {
                           alert(d.msg);
                           window.location.href='/';
                        } else {
                           alert('Server Error: ' + d.msg);
                        }
                        btn.innerHTML = oldText;
                    })
                    .catch(e => {
                        alert('Error: ' + e);
                        btn.innerHTML = oldText;
                    });
                }

                function clearAllAlarms() {
                    if (!confirm('Are you sure you want to clear ALL alarms (ZTE and Huawei)?')) {
                        return;
                    }

                    const btn = event.srcElement || event.target;
                    const oldText = btn.innerHTML;
                    btn.innerHTML = '⏳ Clearing...';

                    fetch('/clear_alarms', {method: 'POST'})
                    .then(r => {
                        if (!r.ok) { throw new Error("HTTP " + r.status); }
                        return r.json();
                    })
                    .then(d => {
                        if(d.status === 'ok') {
                           alert(d.msg);
                           window.location.href='/';
                        } else {
                           alert('Error: ' + d.msg);
                        }
                        btn.innerHTML = oldText;
                    })
                    .catch(e => {
                        alert('Error: ' + e);
                        btn.innerHTML = oldText;
                    });
                }

                function runOfflineCheck() {
                    const fileInput = document.getElementById('offline-excel-file');
                    const file = fileInput.files[0];

                    if (!file) {
                        alert('Пожалуйста, выберите файл!');
                        return;
                    }

                    const btn = event.srcElement || event.target;
                    const oldText = btn.innerHTML;
                    btn.innerHTML = '⏳ Processing...';

                    const formData = new FormData();
                    formData.append('excel_file', file);

                    fetch('/offline_excel_check', {
                        method: 'POST',
                        body: formData
                    })
                    .then(r => {
                        if (!r.ok) { throw new Error("HTTP " + r.status); }
                        return r.json();
                    })
                    .then(d => {
                        btn.innerHTML = oldText;
                        document.getElementById('offline-excel-panel').style.display = 'none';

                        if(d.status === 'ok') {
                           alert(d.msg);
                           window.location.href='/';
                        } else {
                           alert('Error: ' + d.msg);
                        }
                    })
                    .catch(e => {
                        alert('Error: ' + e);
                        btn.innerHTML = oldText;
                    });
                }

                window.onload = () => {
                    const savedVendor = localStorage.getItem('active_vendor') || 'zte';
                    const btnSelector = savedVendor === 'zte' ? '.btn-green' : '.btn-red';
                    const targetBtn = document.querySelector(btnSelector);
                    showV(savedVendor, targetBtn);
                };
            </script>
        <body>
            <div class="card">
                <div class="theme-controls">
                    <button class="theme-btn" onclick="setTheme('light')" title="Light Theme">☀️</button>
                    <button class="theme-btn" onclick="setTheme('dark')" title="Dark Theme">🌙</button>
                    <button class="theme-btn" onclick="setTheme('orange')" title="Light Orange">🟠</button>
                    <button class="theme-btn" onclick="setTheme('pink')" title="Light Pink">🌸</button>
                    <button class="theme-btn" onclick="setTheme('blue')" title="Light Blue">🔵</button>
                    <button class="theme-btn" onclick="setTheme('glaido')" title="Glaido Dark">🟢</button>
                </div>
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 25px;">
                    <div style="display: flex; gap: 15px;">
                        <!-- ZTE Button with inline SVG logo -->
                        <button class="btn btn-gray btn-vendor" onclick="showV('zte', this)" style="display: flex; align-items: center; gap: 4px; padding: 8px 20px;" title="ZTE">
                            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 60" width="80" height="30">
                                <text x="2" y="48" font-family="Arial Black, Arial, sans-serif" font-weight="900" font-size="54" fill="#005BAC" letter-spacing="-2">ZTE</text>
                                <text x="108" y="30" font-family="SimHei, Microsoft YaHei, Arial, sans-serif" font-weight="900" font-size="24" fill="#000000">中兴</text>
                            </svg>
                        </button>
                        <!-- HUAWEI Button with inline SVG logo (red flower) -->
                        <button class="btn btn-gray btn-vendor" onclick="showV('huawei', this)" style="display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 6px 16px; gap: 2px;" title="HUAWEI">
                            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 60 60" width="36" height="36">
                                <g transform="translate(30,30)">
                                    <!-- 4 petals rotated -->
                                    <ellipse cx="0" cy="-13" rx="5.5" ry="11" fill="#CF0A2C" transform="rotate(0)"/>
                                    <ellipse cx="0" cy="-13" rx="5.5" ry="11" fill="#CF0A2C" transform="rotate(45)"/>
                                    <ellipse cx="0" cy="-13" rx="5.5" ry="11" fill="#CF0A2C" transform="rotate(90)"/>
                                    <ellipse cx="0" cy="-13" rx="5.5" ry="11" fill="#CF0A2C" transform="rotate(135)"/>
                                    <ellipse cx="0" cy="-13" rx="5.5" ry="11" fill="#CF0A2C" transform="rotate(180)"/>
                                    <ellipse cx="0" cy="-13" rx="5.5" ry="11" fill="#CF0A2C" transform="rotate(225)"/>
                                    <ellipse cx="0" cy="-13" rx="5.5" ry="11" fill="#CF0A2C" transform="rotate(270)"/>
                                    <ellipse cx="0" cy="-13" rx="5.5" ry="11" fill="#CF0A2C" transform="rotate(315)"/>
                                </g>
                            </svg>
                            <span style="font-family: Arial, sans-serif; font-weight: 700; font-size: 9px; color: #000; letter-spacing: 1.5px;">HUAWEI</span>
                        </button>
                    </div>

                    <h2 style="margin: 0; text-align: center; flex-grow: 1;">
                        BTS MONITORING v30.0
                        <span id="v-badge">LOADING...</span>
                    </h2>
                    
                    <div style="width: 250px;"></div> <!-- Spacer to keep title centered -->
                </div>


                {% for v in ['zte', 'huawei'] %}
                <div class="stats-bar" id="s-{{v}}">
                    {% if data[v] and data[v].stats %}
                    <span>🏗️ WORK: <span class="new-val">NEW-{{ data[v].stats.total_new }}</span> | MOD-{{ data[v].stats.total_mod }}</span>
                    <span>⌛ TESTS: <span class="new-val">NEW-{{ data[v].stats.t_new }}</span> | MOD-{{ data[v].stats.t_mod }}</span>
                    <span>🔋 SENSORS: <span class="new-val">NEW-{{ data[v].stats.s_new }}</span> | MOD-{{ data[v].stats.s_mod }}</span>
                    <span>📩 READY: <span class="new-val">NEW-{{ data[v].stats.sms_new }}</span> | MOD-{{ data[v].stats.sms_mod }}</span>
                    {% endif %}
                </div>
                {% endfor %}




                <div class="nav">
                    <button class="btn btn-gray" onclick="this.innerHTML='Loading...';window.location.href='/refresh'">🔄 REFRESH</button>
                    <button class="btn btn-gray" onclick="generateSelectedSMS()">📩 SMS (SELECTED)</button>
                    <button class="btn btn-gray" onclick="showFullSMS()">📝 SMS (ALL)</button>
                    <button class="btn btn-gray" onclick="document.getElementById('check-p').style.display='block'">🚨 Check Alarms</button>
                    <button class="btn btn-gray" onclick="document.getElementById('offline-excel-panel').style.display='block'"> Offline Check</button>
                    <button class="btn btn-gray" onclick="testFtp()">🔌 FTP Connect</button>
                    <button class="btn btn-gray" onclick="downloadFtp()">🌩️ FTP Process</button>
                    <button class="btn btn-gray" onclick="clearAllAlarms()">🧹 Clear Alarms</button>
                    <button class="btn btn-gray" onclick="location.href='/export_full_report'">📥 Download Excel</button>
                    <button class="btn btn-gray" onclick="window.open('/dashboard', '_blank')">📊 Open Dashboard</button>
                    <button class="btn btn-gray" id="main-table-toggle-btn" onclick="toggleMainTable()">📋 View Main Excel</button>
                </div>


                <div id="sms-storage" style="display:none;">{{ comb_sms }}</div>

                <div id="sms-panel" style="display:none; margin-bottom: 25px; background: var(--bg-color); border: 1px solid var(--accent-green); border-radius: 8px; padding: 25px;">
                    <pre id="t-combined" style="font-family: 'Lora', monospace; font-size:13px; white-space:pre-wrap; background:white; padding:20px; border-radius:8px; border:1px solid var(--gray-light);">{{ comb_sms }}</pre>
                    <div style="display:flex; gap:10px; margin-top:15px;">
                        <button class="btn btn-green" style="flex:1;" onclick="navigator.clipboard.writeText(document.getElementById('t-combined').innerText).then(()=>alert('Copied!'))">📋 COPY TEXT</button>
                        <button class="btn btn-gray" style="flex:1;" onclick="document.getElementById('sms-panel').style.display='none'">✖ CLOSE</button>
                    </div>
                </div>

                {% for v in ['zte', 'huawei'] %}
                <div id="c-{{v}}" class="v-con" style="display:none;">
                    <div class="reg-panel"><div class="reg-grid">{{ data[v].regs|safe }}</div></div>
                    <div style="display:flex; gap:15px; margin-bottom:15px;">
                        <div style="flex:1; background:var(--bg-color); padding:15px; border-radius:8px; font-size:12px; border:1px solid var(--gray-light);"><b>⚙️ Config Waiting:</b> {{ data[v].stats.c_ids }}</div>
                        <div style="flex:1; background:var(--bg-color); padding:15px; border-radius:8px; font-size:12px; border:1px solid var(--gray-light);"><b>⏳ Tests Waiting:</b> {{ data[v].stats.t_ids }}</div>
                    </div>
                    <div style="display:flex; gap:15px; margin-bottom:15px;">
                        <div style="flex:1; background:var(--bg-color); padding:15px; border-radius:8px; font-size:12px; border:1px solid var(--gray-light);"><b>🔧️ Service Waiting:</b> {{ data[v].stats.srv_ids }}</div>
                        <div style="flex:1; background:var(--bg-color); padding:15px; border-radius:8px; font-size:12px; border:1px solid var(--gray-light);"><b>🔋 Sensors Waiting:</b> {{ data[v].stats.s_ids }}</div>
                    </div>
                    <!-- Only the table is collapsible -->
                    <div class="table-section" style="display:none;">
                    <table>
                        <thead>
                            <tr style="height:50px;">
                                <td colspan="6" style="text-align:right; font-weight:bold; font-family:'Poppins';">TOTAL:</td>
                                {{ data[v].counts|safe }}
                                <td></td>
                            </tr>
                            <tr>
                                <th style="width: 40px;"><input type="checkbox" onclick="toggleAll(this)" title="Select All"></th>
                                <th onclick="sortTable(1)">Region ↕</th>
                                <th onclick="sortTable(2)">ID ↕</th>
                                <th onclick="sortTable(3)">Name ↕</th>
                                <th onclick="sortTable(4)">Type ↕</th>
                                <th onclick="sortTable(5)" style="background: var(--gray-light); color: var(--text-dark);">Status ↕</th>
                                {{steps_h|safe}}
                                <th onclick="sortTable(16)">% ↕</th>
                            </tr>
                        </thead>
                        <tbody>{{ data[v].rows|safe }}</tbody>
                    </table>
                    </div>
                </div>
                {% endfor %}

                <div id="check-p" style="display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(20,20,19,0.5); z-index:9999; backdrop-filter: blur(2px);">
                    <div style="background:white; width:500px; margin:100px auto; padding:40px; border-radius:16px; position:relative;">
                        <h3 style="margin-top:0;">🚨 Check Alarms</h3>
                        <form method="post" enctype="multipart/form-data">
                            <div style="margin-bottom:20px;">
                                <label style="font-size:13px; font-weight:600; font-family:'Poppins'; display:block; margin-bottom:5px;">ZTE File (Excel/ZIP):</label>
                                <input type="file" name="zte_alarms" style="width:100%; padding:10px; border:1px solid var(--gray-light); border-radius:6px;">
                            </div>
                            <div style="margin-bottom:25px;">
                                <label style="font-size:13px; font-weight:600; font-family:'Poppins'; display:block; margin-bottom:5px;">Huawei File (Excel/ZIP):</label>
                                <input type="file" name="hw_alarms" style="width:100%; padding:10px; border:1px solid var(--gray-light); border-radius:6px;">
                            </div>
                            <button type="submit" class="btn btn-red" style="width:100%;">🔥 CHECK ALARMS</button>
                        </form>
                        <button class="btn btn-gray" style="width:100%; margin-top:10px;" onclick="document.getElementById('check-p').style.display='none'">CANCEL</button>
                    </div>
                </div>

                <!-- Offline Excel Check Modal -->
                <div id="offline-excel-panel" style="display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(20,20,19,0.5); z-index:9999; backdrop-filter: blur(2px);">
                    <div style="background:var(--card-bg); width:500px; margin:100px auto; padding:40px; border-radius:16px; position:relative; border:1px solid var(--gray-light);">
                        <h3 style="margin-top:0; color:var(--text-dark); font-family:'Poppins';">📄 Offline Excel Check</h3>
                        <p style="font-size:13px; color:var(--gray-mid); margin-bottom:25px;">Проверьте скачанный Excel файл из pm.utc.uz</p>

                        <div style="margin-bottom:25px;">
                            <label style="font-size:13px; font-weight:600; font-family:'Poppins'; display:block; margin-bottom:5px; color:var(--text-dark);">Excel File:</label>
                            <input type="file" id="offline-excel-file" accept=".xlsx,.xls" 
                                   style="width:100%; padding:10px; border:1px solid var(--gray-light); border-radius:6px;">
                        </div>

                        <button class="btn btn-blue" style="width:100%; background:#2d3748;" onclick="runOfflineCheck()">🔍 CHECK FILE</button>
                        <button class="btn btn-gray" style="width:100%; margin-top:10px;" onclick="document.getElementById('offline-excel-panel').style.display='none'">CANCEL</button>
                    </div>
                </div>

                <!-- Graph Modal -->
                <div id="graph-modal" style="display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(20,20,19,0.8); z-index:9999; backdrop-filter: blur(4px);">
                    <div style="background:var(--card-bg); width:90%; height:90%; margin:30px auto; padding:20px; border-radius:16px; position:relative; border:1px solid var(--gray-light); display:flex; flex-direction:column;">
                        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:20px;">
                            <h2 style="margin:0;">📈 Project Progress Graphics</h2>
                            <div style="display:flex; gap:10px;">
                                <select id="graph-period" onchange="loadGraphData()" style="padding:8px 16px; border-radius:8px; border:1px solid var(--gray-light); background:var(--bg-color); color:var(--text-dark); font-family:'Poppins';">
                                    <option value="day">Daily</option>
                                    <option value="week">Weekly</option>
                                    <option value="month">Monthly</option>
                                </select>
                                <button class="btn btn-gray" onclick="document.getElementById('graph-modal').style.display='none'">✖ CLOSE</button>
                            </div>
                        </div>
                        <div style="flex:1; position:relative;">
                            <canvas id="progressChart"></canvas>
                        </div>
                    </div>
                </div>
                <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
                <script>
                    let myChart = null;

                    function showGraphModal() {
                        document.getElementById('graph-modal').style.display = 'block';
                        loadGraphData();
                    }

                    function loadGraphData() {
                        const period = document.getElementById('graph-period').value;
                        fetch(`/api/graph_data?period=${period}`)
                        .then(r => r.json())
                        .then(data => {
                            if(data.error) {
                                alert("Error: " + data.error);
                                return;
                            }
                            renderChart(data);
                        })
                        .catch(e => alert("Error loading graph: " + e));
                    }

                    function renderChart(data) {
                        const ctx = document.getElementById('progressChart').getContext('2d');

                        if(myChart) {
                            myChart.destroy();
                        }

                        myChart = new Chart(ctx, {
                            type: 'line',
                            data: {
                                labels: data.labels,
                                datasets: data.datasets
                            },
                            options: {
                                responsive: true,
                                maintainAspectRatio: false,
                                interaction: {
                                    mode: 'index',
                                    intersect: false,
                                },
                                plugins: {
                                    title: {
                                        display: true,
                                        text: 'Completed Bases Over Time'
                                    },
                                    tooltip: {
                                        mode: 'index',
                                        intersect: false
                                    }
                                },
                                scales: {
                                    x: {
                                        display: true,
                                        title: {
                                            display: true,
                                            text: 'Date'
                                        }
                                    },
                                    y: {
                                        display: true,
                                        title: {
                                            display: true,
                                            text: 'Count'
                                        },
                                        beginAtZero: true
                                    }
                                }
                            }
                        });
                    }
                </script>

                <div id="sms-2fa-modal" style="display:{% if wait_sms %}block{% else %}none{% endif %}; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(20,20,19,0.8); z-index:9999; backdrop-filter: blur(4px);">
                    <div style="background:white; width:400px; margin:150px auto; padding:40px; border-radius:16px; text-align:center;">
                        <h2 style="margin-top:0;">🔐 2FA Required</h2>
                        <p>Enter the 6-digit code sent to your phone:</p>
                        <input type="text" id="sms_input_code" placeholder="123456" style="font-size:24px; text-align:center; padding:10px; width:200px; letter-spacing:4px; margin-bottom:20px;">
                        <br>
                        <button class="btn btn-blue" onclick="submitSMS()">✅ SUBMIT CODE</button>
                    </div>
                    <script>
                        function submitSMS() {
                            const code = document.getElementById('sms_input_code').value;
                            if(!code) return alert("Enter code");

                            fetch('/submit_2fa', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({code:code})})
                            .then(r=>r.json())
                            .then(d=>{
                                if(d.status==='ok') {
                                    alert('Success! Logging in...');
                                    window.location.reload();
                                } else {
                                    alert('Error: ' + d.msg);
                                }
                            });
                        }
                    </script>
                </div>

                <p style="text-align:center; font-size:11px; color:var(--gray-mid); margin-top:40px; font-family:'Poppins';">Sync: {{ last }} | Server: 129.20.0.150:5001</p>
            </div>
        </body>
        </html>
    ''',
                                  data={'zte': z_v, 'huawei': h_v},
                                  alarms=data_store['active_alarms'],
                                  show_modal=show_modal,
                                  comb_sms=data_store['combined_sms'],
                                  last=lu,
                                  steps_h=steps_header)


@app.route('/export_full_report')
def export_full_report():
    if not data_store["full_export_data"]:
        return redirect(url_for('index'))
    df = pd.DataFrame(data_store["full_export_data"])
    df['SortOrder'] = df['Статус'].apply(lambda x: 0 if x != 'Done' else 1)
    df = df.sort_values(by=['SortOrder', 'Вендор', 'Регион']).drop(columns=['SortOrder'])
    cols_order = ['Вендор', 'Регион', 'ID БТС', 'Название', 'Тип объекта', 'Статус'] + \
                 [f'Шаг {i} ({STEP_LABELS[i]})' for i in range(1, 11)] + ['%']
    df = df[cols_order]
    out = BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as wr:
        df.to_excel(wr, index=False)
    out.seek(0)
    return send_file(out, as_attachment=True, download_name=f"Full_Audit_{datetime.now().strftime('%Y%m%d')}.xlsx")


@app.route('/generate_custom_sms', methods=['POST'])
def generate_custom_sms():
    selected_ids = request.json.get('ids', [])
    df_all = pd.DataFrame(data_store.get("full_export_data", []))
    if df_all.empty:
        return {"sms": "Ошибка: Нажмите ОБНОВИТЬ!"}
    df_selected = df_all[df_all['ID БТС'].isin(selected_ids)]
    return {"sms": generate_sms_text(df_selected)}


@app.route('/refresh')
def refresh():
    fetch_and_sync_all()
    return redirect(url_for('index'))


@app.route('/api/refresh_data', methods=['POST'])
def api_refresh_data():
    try:
        print(flush=True, ">>> [API] Refresh requested")
        success = fetch_and_sync_all()
        if success:
            return jsonify({"status": "ok", "msg": "Data refreshed successfully"})
        else:
            return jsonify({"status": "error", "msg": "Failed to refresh data. Check server logs."})
    except Exception as e:
        print(flush=True, f">>> [API] Error during refresh: {e}")
        return jsonify({"status": "error", "msg": str(e)})


@app.route('/api/graph_data')
def api_graph_data():
    try:
        # 1. Check data
        data = data_store.get("full_export_data", [])
        if not data:
            return jsonify({"error": "No data loaded"})

        # 2. Convert to DataFrame
        df = pd.DataFrame(data)

        # 3. Filter for 'Done' or similar status if needed.
        # The user wants to see "completed bases".
        # We can use 'Статус' == 'Done' OR check if CompletionDate is present.
        # Let's use records where CompletionDate is not '-'

        if 'CompletionDate' not in df.columns:
            return jsonify({"error": "Date column missing"})

        df = df[df['CompletionDate'] != '-']

        # 4. Parse Dates
        # Format usually: "23.01.2025 15:30:00" or similar
        # We need to handle potential parsing errors
        df['dt'] = pd.to_datetime(df['CompletionDate'], dayfirst=True, errors='coerce')
        df = df.dropna(subset=['dt'])

        if df.empty:
            return jsonify({"labels": [], "datasets": []})

        # 5. Get period from request
        period = request.args.get('period', 'day')  # day, week, month

        # 6. Grouping
        if period == 'week':
            df['period_str'] = df['dt'].dt.to_period('W').apply(lambda r: r.start_time.strftime('%Y-%m-%d'))
        elif period == 'month':
            df['period_str'] = df['dt'].dt.to_period('M').apply(lambda r: r.start_time.strftime('%Y-%m'))
        else:  # day
            df['period_str'] = df['dt'].dt.strftime('%Y-%m-%d')

        # 7. Aggregate by Vendor + Region
        # We want to show lines for each Vendor (or maybe Vendor-Region? Too messy).
        # User said: "v razreze vendora i v razreze regionov".
        # Maybe stacked bar chart? Or filtered by frontend.
        # Let's send a flexible structure:
        # {
        #   labels: [date1, date2...],
        #   datasets: [ {label: 'Huawei-Toshkent', data: [...]}, ... ]
        # }
        # Only show Top regions or allow filtering on frontend?
        # Let's categorize by Vendor first as requested "Completed bases".

        # Let's return datasets for Vendors (Total) and per Region if needed.
        # For simplicity/mvp: 2 datasets (Huawei, ZTE) accumulated?
        # User asked "v razreze vendora i v razreze regionov".
        # Let's group by [Period, Vendor, Region] -> Count

        grp = df.groupby(['period_str', 'Вендор', 'Регион']).size().reset_index(name='count')

        # Pivot to get: Index=Period, Columns=[Vendor, Region], Values=Count
        # This might be complex to chart.
        # Let's simplify: return the raw grouped data, let frontend parse?
        # Or better: Prepare datasets for ChartJS.

        # Get all unique periods sorted
        all_periods = sorted(grp['period_str'].unique())

        datasets = []

        # Strategy: One dataset per Vendor (total) AND if requested, per region?
        # Interactive graph is better.
        # Let's create datasets for:
        # 1. Huawei Total
        # 2. ZTE Total
        # And user can see breakdown?

        # Actually user wants to see "v razreze regionov".
        # Use simple color coding: Huawei (Red), ZTE (Green).
        # Stacked by Region? Too many regions (14).

        # Let's provide:
        # Datasets: Huawei, ZTE.
        # And a separate data object for specific region breakdown if needed?

        # Revised: The user likely wants to see the trend.
        # Let's return datasets for each Vendor.

        # Group by Period + Vendor
        grp_v = df.groupby(['period_str', 'Вендор']).size().reset_index(name='count')

        colors = {'Huawei': '#FF5722', 'ZTE': '#4CAF50', 'HUAWEI': '#FF5722', 'ZTE': '#4CAF50'}

        for v in grp_v['Вендор'].unique():
            v_data = grp_v[grp_v['Вендор'] == v]
            # Reindex to fill missing periods with 0
            v_data = v_data.set_index('period_str').reindex(all_periods, fill_value=0)

            datasets.append({
                "label": v.upper(),
                "data": v_data['count'].tolist(),
                "borderColor": colors.get(v, '#333'),
                "backgroundColor": colors.get(v, '#333'),
                "fill": False
            })

        return jsonify({
            "labels": all_periods,
            "datasets": datasets,
            # Also send region breakdown per period for detailed view?
            # "detailed": grp.to_dict(orient='records')
        })

    except Exception as e:
        print(flush=True, traceback.format_exc())
        return jsonify({"error": str(e)})


@app.route('/clear_alarms', methods=['POST'])
def clear_alarms():
    """Clear all active alarms (ZTE and Huawei)"""
    try:
        alarm_count = len(data_store.get('active_alarms', {}))
        data_store['active_alarms'] = {}
        print(flush=True, f">>> [CLEAR] Cleared {alarm_count} alarm entries")
        return {"status": "ok", "msg": f"✓ Cleared {alarm_count} alarm entries"}
    except Exception as e:
        print(flush=True, f">>> [CLEAR] Error: {e}")
        return {"status": "error", "msg": str(e)}


@app.route('/ftp_test', methods=['POST'])
def ftp_test():
    try:
        if not os.path.exists(CONFIG_FILE): return {"status": "error", "msg": "Config not found"}
        with open(CONFIG_FILE, "r") as f:
            cfg = json.load(f)
        ftp_cfg = cfg.get('ftp', {})
        if not ftp_cfg: return {"status": "error", "msg": "No FTP config in json"}

        ftp = ftplib.FTP()
        ftp.connect(ftp_cfg.get('host'), int(ftp_cfg.get('port', 21)))
        ftp.login(ftp_cfg.get('user'), ftp_cfg.get('pass'))
        w = ftp.getwelcome()
        ftp.quit()
        return {"status": "ok", "msg": f"Connected! {w}"}
    except Exception as e:
        return {"status": "error", "msg": str(e)}


@app.route('/dashboard')
def dashboard():
    """Integrated dashboard - no separate process needed!"""
    return render_template_string(DASHBOARD_HTML)


@app.route('/api/dashboard_stats')
def dashboard_stats():
    """API endpoint for dashboard data"""
    try:
        vendor_filter = request.args.get('vendor', 'all')

        # Use data from full_export_data - this is populated during sync
        if not data_store.get("full_export_data"):
            return {"error": "No data loaded. Click REFRESH first!"}

        print(flush=True, f">>> [DASHBOARD] Total records in full_export_data: {len(data_store['full_export_data'])}")

        df = pd.DataFrame(data_store["full_export_data"])

        # Debug: show first record structure
        if len(df) > 0:
            print(flush=True, f">>> [DASHBOARD] Sample record keys: {list(df.columns)}")
            print(flush=True, f">>> [DASHBOARD] First record: {df.iloc[0].to_dict()}")

        # Filter by vendor if specified
        if vendor_filter != 'all':
            df = df[df['Вендор'].str.upper() == vendor_filter.upper()]

        print(flush=True, f">>> [DASHBOARD] After vendor filter '{vendor_filter}': {len(df)} records")

        # Calculate stats
        total = len(df)
        done_df_all = df[df['Статус'].str.lower() == 'done']
        done = len(done_df_all)
        done_type = done_df_all['Тип объекта'].astype(str).str.lower()
        done_mod = len(done_type[done_type.str.contains('mod', na=False)])
        done_new = len(done_type[done_type.str.contains('new', na=False)])
        inprogress = len(df[df['Статус'].str.lower() == 'inprogress'])
        sms = len(df[df['Статус'].str.lower() == 'sms'])
        new = len(df[df['Статус'].str.lower() == 'new'])

        # Calculate overall progress as average of % column
        try:
            progress = df['%'].astype(float).mean()
        except:
            progress = 0

        # Group by Region
        regions_data = []
        for reg in df['Регион'].unique():
            if pd.isna(reg) or str(reg) == 'Unknown':
                continue

            reg_df = df[df['Регион'] == reg]
            total_reg = len(reg_df)
            if total_reg == 0:
                continue

            # Calculate progress for region as average of %
            try:
                prog_reg = reg_df['%'].astype(float).mean()
            except:
                prog_reg = 0

            # Count types
            type_col = reg_df['Тип объекта'].astype(str).str.lower()
            cnt_new = len(type_col[type_col.str.contains('new', na=False)])
            cnt_mod = len(type_col[type_col.str.contains('mod', na=False)])

            # Status counts - case insensitive
            status_col = reg_df['Статус'].astype(str).str.lower()
            st_done = len(status_col[status_col == 'done'])
            st_inp = len(status_col[status_col == 'inprogress'])
            st_sms = len(status_col[status_col == 'sms'])
            st_new = len(status_col[status_col == 'new'])

            # Split done by type (MOD vs NEW)
            done_df = reg_df[reg_df['Статус'].astype(str).str.lower() == 'done']
            done_type_col = done_df['Тип объекта'].astype(str).str.lower()
            st_done_mod = len(done_type_col[done_type_col.str.contains('mod', na=False)])
            st_done_new = len(done_type_col[done_type_col.str.contains('new', na=False)])

            regions_data.append({
                "name": reg,
                "total": total_reg,
                "progress": round(prog_reg, 1),
                "type_mod": cnt_mod,
                "type_new": cnt_new,
                "done_mod": st_done_mod,
                "done_new": st_done_new,
                "status_done": st_done,
                "status_inp": st_inp,
                "status_sms": st_sms,
                "status_new": st_new
            })

        # Sort by progress
        regions_data.sort(key=lambda x: x['progress'])

        return {
            "summary": {
                "total": total,
                "done": done,
                "done_mod": done_mod,
                "done_new": done_new,
                "inprogress": inprogress,
                "sms": sms,
                "new": new,
                "progress": round(progress, 2)
            },
            "regions": regions_data
        }
    except Exception as e:
        print(flush=True, f">>> [DASHBOARD] Error: {e}")
        traceback.print_exc()
        return {"error": str(e)}


@app.route('/ftp_process', methods=['POST'])
def ftp_process_route():
    try:
        if not data_store.get("last_sync_dt"):
            return {"status": "error",
                    "msg": "Данные с портала еще не загружены. Подождите 10-15 сек и обновите страницу."}

        print(flush=True, ">>> [FTP] Starting background process...")
        with open(CONFIG_FILE, "r") as f:
            cfg = json.load(f)
        ftp_cfg = cfg.get('ftp', {})

        print(flush=True, f">>> [FTP] Connecting to {ftp_cfg.get('host')}...")
        ftp = ftplib.FTP()
        ftp.connect(ftp_cfg.get('host'), int(ftp_cfg.get('port', 21)))
        ftp.login(ftp_cfg.get('user'), ftp_cfg.get('pass'))

        files = ftp.nlst()
        zips = sorted([f for f in files if f.lower().endswith('.zip')])
        if not zips:
            ftp.quit()
            return {"status": "error", "msg": "No ZIP files on FTP"}

        target = next((f for f in reversed(zips) if 'fm-active' in f.lower()), zips[-1])

        print(flush=True, f">>> [FTP] Downloading {target}...")
        bio = BytesIO()
        ftp.retrbinary(f"RETR {target}", bio.write)
        ftp.quit()
        bio.seek(0)
        print(flush=True, f">>> [FTP] Downloaded {bio.tell()} bytes. Processing...")

        data_store['active_alarms'] = {}
        processed_count = 0
        total_alarms_found = 0

        with zipfile.ZipFile(bio) as z:
            excel_files = [f for f in z.namelist() if f.lower().endswith(('.xlsx', '.xls'))]
            if not excel_files: return {"status": "error", "msg": "No Excel in ZIP"}

            for xls in excel_files:
                print(flush=True, f">>> [FTP] Parsing {xls}...")
                with z.open(xls) as f_in:
                    content = f_in.read()

                # Step 1: Read to detect header only (safe read)
                try:
                    df_raw = pd.read_excel(BytesIO(content), header=None)
                except Exception as e:
                    print(flush=True, f">>> [FTP] Error reading {xls}: {e}")
                    continue

                for vk in ['huawei', 'zte']:
                    try:
                        cols = COLUMN_LOOKUP[f"{'hw' if vk == 'huawei' else 'zt'}_id"]
                        h_idx = -1

                        # Find header row - SCAN ALL ROWS to be safe (matching manual logic)
                        for i, row in df_raw.iterrows():
                            if any(str(v) in cols for v in row.values):
                                h_idx = i;
                                break

                        if h_idx == -1:
                            # print(flush=True, f">>> [FTP] Header not found for {vk} in {xls}")
                            continue

                        # Step 2: Re-read with correct header (matches manual logic)
                        # This ensures checking types are handled correctly
                        df = pd.read_excel(BytesIO(content), skiprows=h_idx)
                        df.columns = [str(c).strip() for c in df.columns]

                        id_col = next(
                            (c for c in COLUMN_LOOKUP[f"{'hw' if vk == 'huawei' else 'zt'}_id"] if c in df.columns),
                            None)
                        msg_col = next(
                            (c for c in COLUMN_LOOKUP[f"{'hw' if vk == 'huawei' else 'zt'}_msg"] if c in df.columns),
                            None)

                        if id_col and msg_col:
                            count_for_file = 0
                            print(flush=True, f">>> [FTP] Processing {vk} in {xls}. Found cols: {id_col}, {msg_col}")

                            for _, r in df.iterrows():
                                # Use exact ID parsing logic from manual loop
                                raw_val = r.get(id_col, "")
                                sid_n, _ = parse_site_info(raw_val)
                                if not sid_n: sid_n = get_num(raw_val)

                                if sid_n in data_store["sms_numeric_ids"][vk]:
                                    fid = data_store["id_bridge"].get(sid_n, sid_n)
                                    msg = str(r.get(msg_col, "---"))
                                    data_store['active_alarms'].setdefault(fid, []).append(msg)
                                    count_for_file += 1

                            print(flush=True, f">>> [FTP] Found {count_for_file} alarms for {vk}.")
                            total_alarms_found += count_for_file
                            processed_count += 1
                    except Exception as e:
                        print(flush=True, f">>> [FTP] Error processing vendor {vk}: {e}")
                        continue

        # Format only NEW alarms (lists), don't re-format already formatted strings
        for k, v in data_store['active_alarms'].items():
            if isinstance(v, list):  # Only format if it's still a list
                data_store['active_alarms'][k] = "<br> • " + "<br> • ".join(list(set(v)))

        print(flush=True, f">>> [FTP] Done. Total matches: {total_alarms_found}")
        return {"status": "ok",
                "msg": f"Processed {target}. Found alarms for {len(data_store['active_alarms'])} sites."}
    except Exception as e:
        traceback.print_exc()
        return {"status": "error", "msg": str(e)}


@app.route('/offline_excel_check', methods=['POST'])
def offline_excel_check():
    """Check Excel file uploaded by user (downloaded from pm.utc.uz manually)"""
    try:
        if 'excel_file' not in request.files:
            return {"status": "error", "msg": "No file uploaded"}

        file = request.files['excel_file']
        if file.filename == '':
            return {"status": "error", "msg": "No file selected"}

        if not file.filename.lower().endswith(('.xlsx', '.xls')):
            return {"status": "error", "msg": "Invalid file type. Please upload Excel file (.xlsx or .xls)"}

        if not data_store.get("sms_numeric_ids") or not any(data_store["sms_numeric_ids"].values()):
            return {"status": "error", "msg": "No initial data. Please click REFRESH first!"}

        print(flush=True, f">>> [OFFLINE] Processing uploaded file: {file.filename}")

        # Read Excel file from upload
        excel_content = file.read()
        df_raw = pd.read_excel(BytesIO(excel_content), engine='openpyxl')

        # Reset active alarms
        data_store['active_alarms'] = {}
        total_alarms = 0

        # Try to process for both vendors
        for vk in ['zte', 'huawei']:
            try:
                # Find header row
                h_idx = next((i for i, r in df_raw.iterrows() if any(
                    x in [str(v) for v in r.values] for x in
                    COLUMN_LOOKUP[f"{'hw' if vk == 'huawei' else 'zt'}_id"])), 0)

                # Extract header manually from row h_idx
                header_row = df_raw.iloc[h_idx].values
                header_names = [str(v).strip() if pd.notna(v) else f"Unnamed_{i}" for i, v in enumerate(header_row)]

                # Read data starting from row h_idx+1
                df = pd.read_excel(BytesIO(excel_content), skiprows=h_idx + 1, header=None, engine='openpyxl')
                df.columns = header_names[:len(df.columns)]

                id_col = next((c for c in COLUMN_LOOKUP[f"{'hw' if vk == 'huawei' else 'zt'}_id"] if c in df.columns),
                              None)
                msg_col = next((c for c in COLUMN_LOOKUP[f"{'hw' if vk == 'huawei' else 'zt'}_msg"] if c in df.columns),
                               None)

                if not id_col or not msg_col:
                    continue

                count_for_vendor = 0
                for _, r in df.iterrows():
                    raw_val = r.get(id_col, "")
                    sid_n, _ = parse_site_info(raw_val)
                    if not sid_n:
                        sid_n = get_num(raw_val)

                    if sid_n in data_store["sms_numeric_ids"][vk]:
                        fid = data_store["id_bridge"].get(sid_n, sid_n)
                        msg = str(r.get(msg_col, "---"))
                        data_store['active_alarms'].setdefault(fid, []).append(msg)
                        count_for_vendor += 1

                print(flush=True, f">>> [OFFLINE] Found {count_for_vendor} alarms for {vk.upper()}")
                total_alarms += count_for_vendor

            except Exception as e:
                print(flush=True, f">>> [OFFLINE] Error processing {vk}: {e}")
                continue

        # Format alarm messages (only NEW alarms in list format)
        for k, v in data_store['active_alarms'].items():
            if isinstance(v, list):  # Only format if it's still a list
                data_store['active_alarms'][k] = "<br> • " + "<br> • ".join(list(set(v)))

        print(flush=True, f">>> [OFFLINE] Done. Total alarms: {total_alarms}")

        return {"status": "ok",
                "msg": f"Processed '{file.filename}'. Found alarms for {len(data_store['active_alarms'])} sites (total: {total_alarms} alarms)."}

    except Exception as e:
        traceback.print_exc()
        return {"status": "error", "msg": str(e)}


@app.route('/check_status')
def check_status():
    return {"waiting_sms": False}


@app.route('/submit_2fa', methods=['POST'])
def submit_2fa():
    try:
        code = request.json.get('code', '').strip()
        if not code or not sys_state.get('session'):
            return {"status": "error", "msg": "Invalid session or code"}

        session = sys_state['session']
        token_2fa = sys_state.get('ver_token')

        data_v = {f"D{i + 1}": code[i] for i in range(len(code)) if i < 6}
        data_v.update({
            'TwoFactorCode': code,
            '__RequestVerificationToken': token_2fa,
            'RememberMachine': 'false', 'RememberMe': 'false'
        })

        print(flush=True, f">>> [SMS] Sending code: {code}")
        res_v = session.post(f"{BASE_URL}/Account/LoginWith2fa?rememberMe=False", data=data_v, verify=False)

        if res_v.status_code == 200 and "/Bts" in res_v.url:
            print(flush=True, ">>> [SMS] Success!")
            sys_state['waiting_sms'] = False
            sys_state['session'] = None  # Clear

            with open(COOKIE_FILE, 'wb') as f:
                pickle.dump(session.cookies, f)

            # Start sync in background or immediately
            threading.Thread(target=process_session_data, args=(session,)).start()

            return {"status": "ok"}
        else:
            return {"status": "error", "msg": "Incorrect Code"}
    except Exception as e:
        return {"status": "error", "msg": str(e)}


if __name__ == '__main__':
    print(flush=True, f">>> [SERVER] Запуск сервера на http://0.0.0.0:{PORT}")
    print(flush=True, ">>> [INFO] Для входа нажмите кнопку ОБНОВИТЬ в браузере")
    print(flush=True, ">>> [DASHBOARD] Dashboard integrated at /dashboard route")

    # Start main app (dashboard is now integrated, no separate process needed!)
    app.run(host='0.0.0.0', port=PORT, debug=True, use_reloader=False)


