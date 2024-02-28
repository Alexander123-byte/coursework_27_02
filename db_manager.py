import psycopg2
import configparser
import os


class DBManager:
    """Класс для управления базой данных."""
    def __init__(self, config_file='config.ini'):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

    def connect(self):
        return psycopg2.connect(
            host=self.config['database']['host'],
            database=self.config['database']['database'],
            user=self.config['database']['user'],
            password=self.config['database']['password']
        )

    def get_companies_and_vacancies_count(self):
        """Получает количество компаний и вакансий для каждой компании."""
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT company_name, COUNT(vacancies_name) AS count_vacancies "
                            f"FROM employers "
                            f"JOIN vacancies USING (employer_id) "
                            f"GROUP BY employers.company_name")
                result = cur.fetchall()
                return result
        finally:
            conn.close()

    def get_all_vacancies(self):
        """Получает все вакансии с информацией о компаниях."""
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT employers.company_name, vacancies.vacancies_name, "
                            f"vacancies.payment, vacancies_url "
                            f"FROM employers "
                            f"JOIN vacancies USING (employer_id)")
                result = cur.fetchall()
                return result
        finally:
            conn.close()

    def get_avg_salary(self):
        """Получает среднюю зарплату по всем вакансиям."""
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT AVG(payment) as avg_payment FROM vacancies")
                result = cur.fetchall()
                return result
        finally:
            conn.close()

    def get_vacancies_with_higher_salary(self):
        """Получает вакансии с зарплатой выше средней."""
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM vacancies "
                            f"WHERE payment > (SELECT AVG(payment) FROM vacancies) ")
                result = cur.fetchall()
                return result
        finally:
            conn.close()

    def get_vacancies_with_keyword(self, keyword):
        """Получает вакансии, содержащие ключевое слово в названии."""
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM vacancies "
                            f"WHERE lower(vacancies_name) LIKE '%{keyword}%' "
                            f"OR lower(vacancies_name) LIKE '%{keyword}'"
                            f"OR lower(vacancies_name) LIKE '{keyword}%';")
                result = cur.fetchall()
                return result
        finally:
            conn.close()
