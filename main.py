
import psycopg2
from pprint import pprint

def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS Person(
                                    id SERIAL PRIMARY KEY,
                                    name VARCHAR(40),
                                    surname VARCHAR(40),
                                    email VARCHAR(40)
                                    );""")        
   
        cur.execute("""CREATE TABLE IF NOT EXISTS Phones(
                                    id SERIAL PRIMARY KEY,
                                    person_id INTEGER REFERENCES Person(id),
                                    tel VARCHAR(12)
                                    );""")
    conn.commit()

def add_person(conn, pname, psurname, pemail, ptel=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO Person(name, surname, email) VALUES(%s, %s, %s) RETURNING id;
            """, (pname, psurname, pemail))
        result = cur.fetchone()
        conn.commit()
        if ptel is not None:
            add_phones(conn, result[0], ptel)
        conn.commit()
        cur.execute("""
        SELECT * FROM Person;
        """)
        pprint(cur.fetchall())

def add_phones(conn, person_id, ptel):
    with conn.cursor() as cur:    
        cur.execute("""
                    SELECT * FROM Phones  WHERE person_id = %s and tel = %s ;
                    """, (person_id, ptel))        
        # result = cur.fetchone()
        # pprint(cur.fetchone())
        if cur.fetchone() == None:
            cur.execute("""
                INSERT INTO Phones(person_id, tel) VALUES(%s, %s) RETURNING id;
                """, (person_id , ptel))
        
        conn.commit()
        # cur.execute("""
        # SELECT * FROM Phones;
        # """)
        # pprint(cur.fetchall())

def update_phones(conn, person_id, tel_id, ptel):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE Phones SET tel=%s WHERE person_id = %s and id=%s;
            """, (ptel, person_id, tel_id))
        conn.commit()
        # cur.execute("""
        #     SELECT * FROM Phones;
        #     """)
        # pprint(cur.fetchall())

def del_phones(conn, person_id, tel_id=None):
    with conn.cursor() as cur:
        if tel_id is None:
            cur.execute("""
                DELETE FROM Phones WHERE person_id=%s;
                """, (person_id,))
            conn.commit()
            # cur.execute("""
            #     SELECT * FROM Phones;
            #     """)
            # print(cur.fetchall()) 
        else:
            cur.execute("""
                DELETE FROM Phones WHERE person_id=%s and id =%s;
                """, (person_id,tel_id))
            conn.commit()
            # cur.execute("""
            #     SELECT * FROM Phones;
            #     """)
            # pprint(cur.fetchall()) 

def del_person(conn, person_id): 
    with conn.cursor() as cur:
        cur.execute("""
                DELETE FROM Person WHERE id=%s;
                """, (person_id,))
        conn.commit()
        # cur.execute("""
        #         SELECT * FROM Person
        #         """)
        # pprint(cur.fetchall()) 

def search_person(conn,name=None, surname=None, email=None, ptel=None):
    where = ''
    if name is not None:
        where = f"name = '{name}'"
    if surname is not None:
        if  len(where) > 0:
            where = f"{where}  and surname = '{surname}'"
        else:
            where = f"surname = '{surname}'"
    if email is not None:
        if len(where) > 0:
            where = f"{where} and email = '{email}'"
        else:
            where = f"email = '{email}'"
     
    with conn.cursor() as cur:
        cur.execute("""
            SELECT * 
            FROM Person
            JOIN Phones on person_id = Person.id 
            WHERE {};
            """.format(where))
        conn.commit()
        # pprint(cur.fetchall()) 

def update_person(conn, person_id, name=None, surname=None, email=None):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT * 
            FROM Person
            WHERE id=%s;
            """, (person_id,))

        if cur.fetchone() != None:
            result = cur.fetchone()
            if name == None:
                name = result[1]
            if surname == None:
                surname = result[2]
            if email == None:
                email = result[3]
        
            cur.execute("""
                UPDATE Person SET name = %s, surname = %s, email = %s WHERE id = %s;
                """, (name, surname , email , person_id))
            conn.commit()

            cur.execute("""
                SELECT * 
                FROM Person
                WHERE id=%s;
                """, (person_id,))
            # pprint(cur.fetchall())     

def print_all_person(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.name, p.surname, p.email , t.id,  t.tel 
            FROM Person p 
            Left JOIN Phones t on t.person_id = p.id
            """)
        pprint(cur.fetchall())


if __name__ == "__main__":
    conn = psycopg2.connect(database = "netology_db", user="postgres", password="12121987")
    create_table(conn)
    print('Наши данные: id клиента / имя / фамилия / email / id телефона / телефон')
    # print_all_person(conn)
    # add_person(conn, 'ИВАН', 'ИВАНОВ', 'IV@mail.ru', '589-658')
    # pprint('~~~~')
    # add_phones(conn, 28, '22-22-22')
    # pprint('~~~~')
    # update_phones(conn, 26, 24, '66666666')
    # pprint('~~~~')
    # del_phones(conn, 1)
    # pprint('~~~~')
    # del_person(conn, 39)
    # pprint('~~~~')
    # search_person(conn, surname='Тимгановаdfd')
    update_person(conn,26,email='Tanya@mail.ru')
    print('РЕЗУЛЬТАТ:')
    print_all_person(conn)
    pprint('~~~~')
    conn.close()