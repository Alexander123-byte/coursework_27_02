import psycopg2
import requests
import os


def get_vacancies(employer_id):
    """
    Получает список вакансий для указанного работодателя с помощью API hh.ru.

    :param employer_id: Идентификатор работодателя.
    :return: Список словарей с данными о вакансиях.
    """
    params = {
        'area': 1,
        'page': 0,
        'per_page': 10
    }
    url = f"https://api.hh.ru/vacancies?employer_id={employer_id}"
    data_vacancies = requests.get(url, params=params).json()

    vacancies_data = []
    for item in data_vacancies["items"]:
        hh_vacancies = {
            'vacancy_id': int(item['id']),
            'vacancies_name': item['name'],
            'payment': item["salary"]["from"] if item["salary"] else None,
            'requirement': item['snippet']['requirement'],
            'vacancies_url': item['alternate_url'],
            'employer_id': employer_id
        }
        if hh_vacancies['payment'] is not None:
            vacancies_data.append(hh_vacancies)

        return vacancies_data


def get_employer(employer_id):
    """
    Получает информацию о работодателе с помощью API hh.ru.

    :param employer_id: Идентификатор работодателя.
    :return: Словарь с данными о работодателе.
    """
    url = f"https://api.hh.ru/employers/{employer_id}"
    data_vacancies = requests.get(url).json()
    hh_company = {
        "employer_id": int(employer_id),
        "company_name": data_vacancies['name'],
        "open_vacancies": data_vacancies['open_vacancies']
        }

    return hh_company


def create_table():
    """
    Создает таблицы "employers" и "vacancies" в базе данных PostgreSQL.
    """
    conn = psycopg2.connect(host=os.environ.get('DB_HOST'),
                            database=os.environ.get('DB_NAME'),
                            user=os.environ.get('DB_USERNAME'),
                            password=os.environ.get('DB_PASSWORD'))
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("DROP DATABASE IF EXISTS course_work_5")
    cur.execute("CREATE DATABASE course_work_5")

    conn.close()

    conn = psycopg2.connect(host=os.environ.get('DB_HOST'),
                            database=os.environ.get('DB_NAME'),
                            user=os.environ.get('DB_USERNAME'),
                            password=os.environ.get('DB_PASSWORD'))
    with conn.cursor() as cur:
        cur.execute("""
                    CREATE TABLE employers (
                    employer_id INTEGER PRIMARY KEY,
                    company_name varchar(255),
                    open_vacancies INTEGER
                    )""")

        cur.execute("""
                    CREATE TABLE vacancies (
                    vacancy_id SERIAL PRIMARY KEY,
                    vacancies_name varchar(255),
                    payment INTEGER,
                    requirement TEXT,
                    vacancies_url TEXT,
                    employer_id INTEGER REFERENCES employers(employer_id)
                    )""")
    conn.commit()
    conn.close()


def add_to_table(employers_list):
    """
    Добавляет данные о работодателях и их вакансиях в таблицы базы данных PostgreSQL.

    :param employers_list: Список идентификаторов работодателей.
    """
    with psycopg2.connect(host=os.environ.get('DB_HOST'),
                          database=os.environ.get('DB_NAME'),
                          user=os.environ.get('DB_USERNAME'),
                          password=os.environ.get('DB_PASSWORD')) as conn:
        with conn.cursor() as cur:
            cur.execute('TRUNCATE TABLE employers, vacancies RESTART IDENTITY;')

            for employer in employers_list:
                employer_list = get_employer(employer)
                cur.execute('INSERT INTO employers (employer_id, company_name, open_vacancies) '
                            'VALUES (%s, %s, %s) RETURNING employer_id',
                            (employer_list['employer_id'], employer_list['company_name'],
                             employer_list['open_vacancies']))

            for employer in employers_list:
                vacancy_list = get_vacancies(employer)
                for v in vacancy_list:
                    cur.execute('INSERT INTO vacancies (vacancy_id, vacancies_name, '
                                'payment, requirement, vacancies_url, employer_id) '
                                'VALUES (%s, %s, %s, %s, %s, %s)',
                                (v['vacancy_id'], v['vacancies_name'], v['payment'],
                                 v['requirement'], v['vacancies_url'], v['employer_id']))

        conn.commit()



