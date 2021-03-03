import psycopg2

def connection():
    try:
        ps_connection = psycopg2.connect(user="postgres",
                                         password="mysecretpassword",
                                         host="localhost",
                                         port="5432",
                                         database="postgres")

        return ps_connection

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to PostgreSQL", error)

def close(connection, cur):
    if connection:
        cur.close()
        connection.close()
        print("PostgreSQL connection is closed")


if __name__== '__main__':
    conn=connection()
    cur=conn.cursor()
    close(conn,cur)