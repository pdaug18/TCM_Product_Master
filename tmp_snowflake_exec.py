import os
import re
import configparser
from pathlib import Path
import traceback
import snowflake.connector

base = Path('.')
sql_path = base / 'Data_GOV' / 'DG_Col_type_sample.sql'

if not sql_path.exists():
    print(f"ERROR: SQL file not found: {sql_path}")
    raise SystemExit(1)

password = None

candidates = [
    base / 'config' / 'snowflake_connect.config',
    base / 'config' / 'snowflake_connect.ini',
    base / '.env',
    Path('C:/Users/ppatel/Projects/snowflake_connect.config'),
]

for p in candidates:
    if password or not p.exists():
        continue
    text = p.read_text(encoding='utf-8', errors='ignore')

    cp = configparser.ConfigParser()
    try:
        cp.read_string(text)
        for sec in cp.sections():
            for key in cp[sec]:
                if key.lower() in {'password', 'pwd'} and cp[sec][key].strip():
                    password = cp[sec][key].strip().strip('"').strip("'")
                    break
            if password:
                break
    except Exception:
        pass

    if not password:
        m = re.search(r'(?im)^\s*(?:SNOWFLAKE_)?(?:PASSWORD|PWD)\s*[:=]\s*(["\']?)(.+?)\1\s*$', text)
        if m:
            password = m.group(2).strip()

if not password:
    for k in ['SNOWFLAKE_PASSWORD', 'PASSWORD', 'SNOWFLAKE_PWD', 'PWD']:
        v = os.getenv(k)
        if v:
            password = v
            break

if not password:
    print('ERROR: Password not found in local config or environment.')
    raise SystemExit(1)

conn = None
try:
    sql_text = sql_path.read_text(encoding='utf-8', errors='ignore')
    conn = snowflake.connector.connect(
        account='A0281095454371-NF95410',
        user='SNOWPARK_USER',
        role='SNOWPARK_ROLE',
        warehouse='ELT_DEFAULT',
        password=password,
    )
    cursors = conn.execute_string(sql_text)
    if not cursors:
        print('No cursors returned by execute_string.')
    else:
        final_cursor = cursors[-1]
        cols = [d[0] for d in (final_cursor.description or [])]
        print('FINAL_CURSOR_COLUMNS:', cols)
        print('FIRST_20_ROWS:')
        for i, row in enumerate(final_cursor.fetchmany(20), 1):
            print(f'{i}: {row}')
except Exception:
    print('FULL_ERROR_TEXT:')
    print(traceback.format_exc())
finally:
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
