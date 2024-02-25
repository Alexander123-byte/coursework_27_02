import psycopg2


class DBManager:
    """Класс для управления базой данных."""

    def get_companies_and_vacancies_count(self):
        """Получает количество компаний и вакансий для каждой компании."""
        with psycopg2.connect(host="localhost", database="coursework_27_02",
                              user="postgres", password="rewty76") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT company_name, COUNT(vacancies_name) AS count_vacancies  "
                            f"FROM employers "
                            f"JOIN vacancies USING (employer_id) "
                            f"GROUP BY employers.company_name")
                result = cur.fetchall()
            conn.commit()
        return result

    def get_all_vacancies(self):
        """Получает все вакансии с информацией о компаниях."""
        with psycopg2.connect(host="localhost", database="coursework_27_02",
                              user="postgres", password="rewty76") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT employers.company_name, vacancies.vacancies_name, "
                            f"vacancies.payment, vacancies_url "
                            f"FROM employers "
                            f"JOIN vacancies USING (employer_id)")
                result = cur.fetchall()
            conn.commit()
        return result

    def get_avg_salary(self):
        """Получает среднюю зарплату по всем вакансиям."""
        with psycopg2.connect(host="localhost", database="coursework_27_02",
                              user="postgres", password="rewty76") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT AVG(payment) as avg_payment FROM vacancies ")
                result = cur.fetchall()
            conn.commit()
        return result

    def get_vacancies_with_higher_salary(self):
        """Получает вакансии с зарплатой выше средней."""
        with psycopg2.connect(host="localhost", database="coursework_27_02",
                              user="postgres", password="rewty76") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM vacancies "
                            f"WHERE payment > (SELECT AVG(payment) FROM vacancies) ")
                result = cur.fetchall()
            conn.commit()
        return result

    def get_vacancies_with_keyword(self, keyword):
        """Получает вакансии, содержащие ключевое слово в названии."""
        with psycopg2.connect(host="localhost", database="coursework_27_02",
                              user="postgres", password="rewty76") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM vacancies "
                            f"WHERE lower(vacancies_name) LIKE '%{keyword}%' "
                            f"OR lower(vacancies_name) LIKE '%{keyword}'"
                            f"OR lower(vacancies_name) LIKE '{keyword}%';")
                result = cur.fetchall()
            conn.commit()
        return result
