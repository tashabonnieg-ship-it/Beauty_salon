import pandas as pd
import pandasql as psql


file_path1 = r"C:\Users\Admin\Desktop\massage\claims.csv"
file_path2 = r"C:\Users\Admin\Desktop\massage\clients.csv"
file_path3 = r"C:\Users\Admin\Desktop\massage\procedures.csv"
file_path4 = r"C:\Users\Admin\Desktop\massage\sessions.csv"

# Чтение CSV файлов с указанием разделителя ;
df1 = pd.read_csv(file_path1, encoding='cp1251', sep=';')
df2 = pd.read_csv(file_path2, encoding='cp1251', sep=';')
df3 = pd.read_csv(file_path3, encoding='cp1251', sep=';')
df4 = pd.read_csv(file_path4, encoding='cp1251', sep=';')


# 1. Доход с клиента за все процедуры в одну дату
query_daily_client_revenue = """
SELECT 
    s.client_id,
    c.client_name,
    s.date,
    SUM(p.price) as total_revenue_per_day
FROM df4 s
JOIN df3 p ON s.procedure_id = p.procedure_id
JOIN df2 c ON s.client_id = c.client_id
GROUP BY s.client_id, c.client_name, s.date
ORDER BY s.date, total_revenue_per_day DESC
"""

daily_client_revenue = psql.sqldf(query_daily_client_revenue)
print("=== Доход с каждого клиента по дням ===")
print(daily_client_revenue.head(10))

# 2. Общий доход по всем клиентам в каждую дату
query_daily_total_revenue = """
SELECT 
    s.date,
    SUM(p.price) as total_daily_revenue,
    COUNT(DISTINCT s.client_id) as unique_clients,
    COUNT(s.session_id) as total_procedures
FROM df4 s
JOIN df3 p ON s.procedure_id = p.procedure_id
GROUP BY s.date
ORDER BY s.date
"""

daily_total_revenue = psql.sqldf(query_daily_total_revenue)
print("\n=== Общий доход по дням ===")
print(daily_total_revenue.head(10))

# 3. Анализ по месяцам
query_monthly_analysis = """
SELECT 
    SUBSTR(s.date, 4, 7) as month_year,  -- предполагаем формат DD.MM.YYYY
    SUM(p.price) as monthly_revenue,
    SUM(p.duration) as total_minutes,
    COUNT(DISTINCT s.client_id) as unique_clients,
    COUNT(s.session_id) as total_procedures,
    ROUND(SUM(p.price) / (SUM(p.duration) / 60.0), 2) as revenue_per_hour
FROM df4 s
JOIN df3 p ON s.procedure_id = p.procedure_id
GROUP BY month_year
ORDER BY month_year
"""

monthly_analysis = psql.sqldf(query_monthly_analysis)
print("\n=== Анализ по месяцам ===")
print(monthly_analysis.transpose())

# 4. Самая продаваемая услуга в каждом месяце
query_top_procedure_monthly = """
WITH MonthlyProcedureStats AS (
    SELECT 
        SUBSTR(s.date, 4, 7) as month_year,
        p.procedure_id,
        p.type_procedure,
        COUNT(s.session_id) as procedure_count,
        SUM(p.price) as procedure_revenue,
        ROW_NUMBER() OVER (PARTITION BY SUBSTR(s.date, 4, 7) ORDER BY COUNT(s.session_id) DESC) as rank_by_count,
        ROW_NUMBER() OVER (PARTITION BY SUBSTR(s.date, 4, 7) ORDER BY SUM(p.price) DESC) as rank_by_revenue
    FROM df4 s
    JOIN df3 p ON s.procedure_id = p.procedure_id
    GROUP BY month_year, p.procedure_id, p.type_procedure
)
SELECT 
    month_year,
    procedure_id,
    type_procedure,
    procedure_count,
    procedure_revenue
FROM MonthlyProcedureStats 
WHERE rank_by_count = 1
ORDER BY month_year
"""

top_procedures_monthly = psql.sqldf(query_top_procedure_monthly)
print("\n=== Самая продаваемая услуга по месяцам ===")
print(top_procedures_monthly)

# 5. Самый частый клиент в каждом месяце (учитываем уникальные даты)
query_top_client_monthly = """
WITH ClientVisits AS (
    -- Сначала получаем уникальные даты посещений для каждого клиента
    SELECT 
        SUBSTR(s.date, 4, 7) as month_year,
        s.client_id,
        c.client_name,
        s.date as visit_date,
        SUM(p.price) as daily_spent  -- сумма за все процедуры в этот день
    FROM df4 s
    JOIN df3 p ON s.procedure_id = p.procedure_id
    JOIN df2 c ON s.client_id = c.client_id
    GROUP BY month_year, s.client_id, c.client_name, s.date
),
MonthlyClientStats AS (
    -- Затем агрегируем по клиентам и месяцам
    SELECT 
        month_year,
        client_id,
        client_name,
        COUNT(DISTINCT visit_date) as unique_visit_days,  -- уникальные дни посещения
        SUM(daily_spent) as total_spent,                  -- общая сумма за месяц
        ROW_NUMBER() OVER (PARTITION BY month_year ORDER BY COUNT(DISTINCT visit_date) DESC) as rank_by_visits,
        ROW_NUMBER() OVER (PARTITION BY month_year ORDER BY SUM(daily_spent) DESC) as rank_by_spending
    FROM ClientVisits
    GROUP BY month_year, client_id, client_name
)
SELECT 
    month_year,
    client_id,
    client_name,
    unique_visit_days as visit_count,
    total_spent
FROM MonthlyClientStats 
WHERE rank_by_visits = 1
ORDER BY month_year
"""

top_clients_monthly = psql.sqldf(query_top_client_monthly)
print("\n=== Самые частые клиенты по месяцам (по уникальным дням) ===")
print(top_clients_monthly)

# 6. Детальный анализ рабочего времени по месяцам
query_work_time_analysis = """
SELECT 
    SUBSTR(s.date, 4, 7) as month_year,
    SUM(p.duration) as total_minutes,
    ROUND(SUM(p.duration) / 60.0, 2) as total_hours,
    COUNT(s.session_id) as total_procedures,
    ROUND(AVG(p.duration), 2) as avg_procedure_minutes,
    SUM(p.price) as total_revenue,
    ROUND(SUM(p.price) / (SUM(p.duration) / 60.0), 2) as revenue_per_hour
FROM df4 s
JOIN df3 p ON s.procedure_id = p.procedure_id
GROUP BY month_year
ORDER BY month_year
"""

work_time_analysis = psql.sqldf(query_work_time_analysis)
print("\n=== Анализ рабочего времени и доходности ===")
print(work_time_analysis.transpose())

# 7. Сводная таблица по всем метрикам
query_summary = """
WITH MonthlyBase AS (
    -- Базовая статистика по месяцам
    SELECT 
        SUBSTR(s.date, 4, 7) as month_year,
        SUM(p.price) as monthly_revenue,
        SUM(p.duration) as total_minutes,
        COUNT(DISTINCT s.client_id) as unique_clients,
        COUNT(s.session_id) as total_procedures
    FROM df4 s
    JOIN df3 p ON s.procedure_id = p.procedure_id
    GROUP BY month_year
),
PopularProcedures AS (
    -- Самые популярные услуги по месяцам
    SELECT 
        SUBSTR(s.date, 4, 7) as month_year,
        p.type_procedure,
        ROW_NUMBER() OVER (PARTITION BY SUBSTR(s.date, 4, 7) ORDER BY COUNT(s.session_id) DESC) as procedure_rank
    FROM df4 s
    JOIN df3 p ON s.procedure_id = p.procedure_id
    GROUP BY month_year, p.type_procedure
),
FrequentClientsByProcedures AS (
    -- Самые частые клиенты по месяцам (по количеству процедур) - ИСПРАВЛЕНО
    SELECT 
        SUBSTR(s.date, 4, 7) as month_year,
        c.client_name,
        COUNT(s.session_id) as procedure_count,
        RANK() OVER (PARTITION BY SUBSTR(s.date, 4, 7) ORDER BY COUNT(s.session_id) DESC) as client_proc_rank
    FROM df4 s
    JOIN df2 c ON s.client_id = c.client_id
    GROUP BY month_year, c.client_name
),
TopClientsByProcedures AS (
    -- Все топовые клиенты по процедурам (могут быть несколько) - ИСПРАВЛЕНО
    SELECT 
        month_year,
        GROUP_CONCAT(client_name, ', ') as top_clients_by_procedures,
        MAX(procedure_count) as max_procedure_count
    FROM FrequentClientsByProcedures 
    WHERE client_proc_rank = 1
    GROUP BY month_year
),
FrequentClientsByDays AS (
    -- Самые частые клиенты по месяцам (по уникальным дням посещения) - ИСПРАВЛЕНО
    SELECT 
        SUBSTR(s.date, 4, 7) as month_year,
        c.client_name,
        COUNT(DISTINCT s.date) as unique_visit_days,
        RANK() OVER (PARTITION BY SUBSTR(s.date, 4, 7) ORDER BY COUNT(DISTINCT s.date) DESC) as client_days_rank
    FROM df4 s
    JOIN df2 c ON s.client_id = c.client_id
    GROUP BY month_year, c.client_name
),
TopClientsByDays AS (
    -- Все топовые клиенты по дням (могут быть несколько) - ИСПРАВЛЕНО
    SELECT 
        month_year,
        GROUP_CONCAT(client_name, ', ') as top_clients_by_days,
        MAX(unique_visit_days) as max_visit_days
    FROM FrequentClientsByDays 
    WHERE client_days_rank = 1
    GROUP BY month_year
)
SELECT 
    mb.month_year,
    -- Доходность
    mb.monthly_revenue,
    ROUND(mb.monthly_revenue / (mb.total_minutes / 60.0), 2) as revenue_per_hour,

    -- Рабочее время
    mb.total_minutes,
    ROUND(mb.total_minutes / 60.0, 2) as total_hours,

    -- Активность
    mb.unique_clients,
    mb.total_procedures,
    ROUND(mb.total_procedures * 1.0 / mb.unique_clients, 2) as avg_procedures_per_client,

    -- Самая популярная услуга (по количеству процедур)
    pp.type_procedure as most_popular_procedure,

    -- Самые частые клиенты (по количеству процедур) - ИСПРАВЛЕНО
    tcp.top_clients_by_procedures as most_frequent_clients_by_procedures,
    tcp.max_procedure_count as client_procedure_count,

    -- Самые частые клиенты (по уникальным дням посещения) - ИСПРАВЛЕНО
    tcd.top_clients_by_days as most_frequent_clients_by_days,
    tcd.max_visit_days as visit_days_count

FROM MonthlyBase mb
LEFT JOIN PopularProcedures pp ON mb.month_year = pp.month_year AND pp.procedure_rank = 1
LEFT JOIN TopClientsByProcedures tcp ON mb.month_year = tcp.month_year
LEFT JOIN TopClientsByDays tcd ON mb.month_year = tcd.month_year
ORDER BY mb.month_year
"""

summary = psql.sqldf(query_summary)
print("\n=== Сводная таблица по месяцам (с учетом ничьих) ===")
print(summary.transpose())


# ВИЗУАЛИЗАЦИЯ ВСЕХ ДАННЫХ ИЗ ПУНКТА 7 НА ОДНОМ ЛИСТЕ (КОМПАКТНАЯ ВЕРСИЯ)

import matplotlib.pyplot as plt
import numpy as np

# Создаем компактную фигуру
fig, axes = plt.subplots(2, 2, figsize=(12, 10))  # Уменьшил размер
fig.suptitle('📊 АНАЛИЗ ЭФФЕКТИВНОСТИ САЛОНА', fontsize=14, fontweight='bold', y=0.95)  # Поднял заголовок

# Упрощаем подписи месяцев
months_short = [month.replace('2025', "'25") for month in summary['month_year']]

# 1. ДОХОД ПО МЕСЯЦАМ (левый верхний)
bars1 = axes[0, 0].bar(months_short, summary['monthly_revenue'],
                       color='lightblue', edgecolor='navy', alpha=0.8)
axes[0, 0].set_title('💰 ДОХОД', fontweight='bold', fontsize=12)
axes[0, 0].set_ylabel('Рубли', fontsize=10)
axes[0, 0].tick_params(axis='x', rotation=45, labelsize=9)
axes[0, 0].tick_params(axis='y', labelsize=9)
axes[0, 0].grid(axis='y', alpha=0.3)

# 2. ДОХОДНОСТЬ В ЧАС (правый верхний)
axes[0, 1].plot(months_short, summary['revenue_per_hour'],
                marker='o', linewidth=2, color='coral', markersize=4)
axes[0, 1].set_title('📈 ДОХОД/ЧАС', fontweight='bold', fontsize=12)
axes[0, 1].set_ylabel('Руб./час', fontsize=10)
axes[0, 1].tick_params(axis='x', rotation=45, labelsize=9)
axes[0, 1].tick_params(axis='y', labelsize=9)
axes[0, 1].grid(True, alpha=0.3)

# 3. АКТИВНОСТЬ КЛИЕНТОВ (левый нижний)
x = np.arange(len(months_short))
width = 0.35
bars3a = axes[1, 0].bar(x - width/2, summary['unique_clients'], width,
                       label='Клиенты', color='gold', alpha=0.8)
bars3b = axes[1, 0].bar(x + width/2, summary['total_procedures'], width,
                       label='Процедуры', color='orange', alpha=0.8)
axes[1, 0].set_title('👥 АКТИВНОСТЬ', fontweight='bold', fontsize=12)
axes[1, 0].set_ylabel('Количество', fontsize=10)
axes[1, 0].set_xticks(x)
axes[1, 0].set_xticklabels(months_short, rotation=45, fontsize=9)
axes[1, 0].tick_params(axis='y', labelsize=9)
axes[1, 0].legend(fontsize=8)
axes[1, 0].grid(axis='y', alpha=0.3)

# 4. РАБОЧЕЕ ВРЕМЯ (правый нижний)
bars4 = axes[1, 1].bar(months_short, summary['total_hours'],
                       color='lightgreen', alpha=0.8)
axes[1, 1].set_title('⏱️ РАБОЧИЕ ЧАСЫ', fontweight='bold', fontsize=12)
axes[1, 1].set_ylabel('Часы', fontsize=10)
axes[1, 1].tick_params(axis='x', rotation=45, labelsize=9)
axes[1, 1].tick_params(axis='y', labelsize=9)
axes[1, 1].grid(axis='y', alpha=0.3)

# Настраиваем отступы
plt.tight_layout()
plt.subplots_adjust(top=0.90, hspace=0.5, wspace=0.4)  # Увеличил расстояния

# Сохраняем в файл
plt.savefig('анализ_салона.png', dpi=300, bbox_inches='tight')
plt.show()

print("\n" + "="*50)
print("СВОДКА ПО МЕСЯЦАМ")
print("="*50)

for _, row in summary.iterrows():
    print(f"\n {row['month_year']}:")
    print(f"   Доход: {row['monthly_revenue']:,.0f} руб.")
    print(f"   Доход/час: {row['revenue_per_hour']:,.0f} руб.")
    print(f"   Клиенты: {row['unique_clients']}, Процедуры: {row['total_procedures']}")
    print(f"   Часы работы: {row['total_hours']:.1f} ч")
    print(f"   Топ клиенты: {row['most_frequent_clients_by_days']}")

print("\n" + "="*50)
print("ЛУЧШИЕ ПОКАЗАТЕЛИ")
print("="*50)
best_month_revenue = summary.loc[summary['monthly_revenue'].idxmax()]
best_month_hourly = summary.loc[summary['revenue_per_hour'].idxmax()]

print(f"Лучший по доходу: {best_month_revenue['month_year']} - {best_month_revenue['monthly_revenue']:,.0f} руб.")
print(f"Лучший по доход/час: {best_month_hourly['month_year']} - {best_month_hourly['revenue_per_hour']:,.0f} руб.")
print(f"Самые частые клиенты: {summary['most_frequent_clients_by_days'].iloc[-1]}")
print(f"Популярные услуги: {', '.join(summary['most_popular_procedure'].unique())}")
