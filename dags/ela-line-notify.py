import requests
import json
import pytz
import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def get_data_report_today():
    ELA_URL = 'http://172.16.4.146/api/leave/line_notify'
    ELA_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwYXNzd29yZCI6ImxlYXZlQDIwMTkifQ.VlVaVGyGYIhkASq4wiAO6HzWtAVe4YxjBf3PgAGu_Zo'
    params = {
        'today': str(datetime.date.today())
    }
    headers = {
        'Authorization': 'Bearer '+ELA_TOKEN
    }
    response = requests.get(ELA_URL, params=params, headers=headers)

    data = response.json()
    with open('data.json', 'w') as f:
        json.dump(data, f)

    return data


def send_line_notify():
    LINE_URL = 'https://notify-api.line.me/api/notify'
    # ZRjKkpQE11WLZrjiNkX6LJWLY1CRgC9gdbFY60btNGJ
    LINE_TOKEN = 'cvUqqWasewk9bCNlk4C30cmluh6CTZytSPUzY2rxn0x'
    headers = {
        'content-type': 'application/x-www-form-urlencoded',
        'Authorization': 'Bearer '+LINE_TOKEN
    }

    # {0} - วันที่ปัจจุบัน
    header = """\n รายชื่อลางาน ประจำวันที่ {0} ดังนี้
    """.format(get_date_thai())

    #{0} - ลำดับ,{1} - ชื่อ,{2} - นามสกุล
    body_format = """{0}. {1} {2}
    """

    body = ""
    count = 1
    with open('data.json') as f:
        data = json.load(f)

    if data:
        for list in data:
            body += body_format.format(count,
                                       list["first_name"], list["last_name"])
            count += 1

        msg = header + body

        r = requests.post(LINE_URL, headers=headers, data={'message': msg})
        print(r.text)


def get_date_thai():
    tz = pytz.timezone('Asia/Bangkok')
    now1 = datetime.datetime.now(tz)
    month_name = 'x มกราคม กุมภาพันธ์ มีนาคม เมษายน พฤษภาคม มิถุนายน กรกฎาคม สิงหาคม กันยายน ตุลาคม พฤศจิกายน ธันวาคม'.split()[
        now1.month]
    thai_year = now1.year + 543
    return "%d %s %d" % (now1.day, month_name, thai_year)  # 30 ตุลาคม 2560


default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2021, 2, 28, 0, 0, 0),
    'email': ['tanakrit@hospital-os.com'],
}
with DAG('eLeave_Management_line_notify',
         schedule_interval='0 9 * * *',
         default_args=default_args,
         description='A data pipeline for eLeave Management',
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='get_data_report_today',
        python_callable=get_data_report_today
    )

    t2 = PythonOperator(
        task_id='send_line_notify',
        python_callable=send_line_notify
    )

    t1 >> t2
