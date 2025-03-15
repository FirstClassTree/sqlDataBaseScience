from queries_db_script import *
import pandas as pd
import mysql.connector

i = 0
def print_query(results,cursor):
    global i
    print(f"Query number {(i//3)+1}, Example number {(i%3)+1}:") 
    i+=1
    columns_name = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results,columns=columns_name)
    pd.set_option('display.max_colwidth', 25)
    print(df)

def query_1_example(cursor,connection):
    #word searched in description, average_vote thershold:
    
    results = query_1(cursor,connection,"hero",7.5)
    print_query(results,cursor)
    print("** Incredibles 2... maybe make Incredibles 3?**")
    results = query_1(cursor,connection,"love",8.3)
    print_query(results,cursor)
    results = query_1(cursor,connection,"journey",8.1)
    print_query(results,cursor)
    print("** finding nemo has a very high revenue and rating might want to consider making another fish journey movie...**")

    return
def query_2_example(cursor,connection):
    #prefix searched in title, total_vote thershold
    #returns starwars and star Strek films and shows their revenue
    results = query_2(cursor,connection,"star",571847)
    print_query(results,cursor)
    results = query_2(cursor,connection,"tom",171847)
    print_query(results,cursor)
    results = query_2(cursor,connection,"car",251828)
    print_query(results,cursor)


    return
def query_3_example(cursor,connection):
    # minimum amount of films in actor carrer, movie_name, limit of top actors returned
    # returns top actors average revenue in movies they played
     
    results = query_3(cursor,connection,5,"Guardians of the Galaxy Vol. 2",5)
    print_query(results,cursor)
    print("**Karen Gillan 703m dollar average film revenue is very high maybe we should hire her**")
    results = query_3(cursor,connection,2,"Fight Club",2)
    print_query(results,cursor)
    results = query_3(cursor,connection,7,"The Lord of the Rings: The Fellowship of the Ring",3)
    print_query(results,cursor)

    
    return
def query_4_example(cursor,connection):
    #films genere, minimum number of director films, limit on amount of directors
    # returns average actors director hired in films in particaulr generes revenue
    results = query_4(cursor,connection,"western",2,2)
    print_query(results,cursor)
    print("** Clint Eastwood guy seems pretty good in directing westerns**")
    results = query_4(cursor,connection,"action",8,3)
    print_query(results,cursor)
    print(" ** The top 1 is for this query is Michael Bay so that is reasonable:**")
    results = query_4(cursor,connection,"comedy",13,2)
    print_query(results,cursor)
    # Apperently garray marshell films have a better actor genre fit then woody ellen, sort of

    return
def query_5_example(cursor,connection):
    # start_date, end_date, results limit
    # returns top grossing film in the time window along with most popular film in that genre in that time
    results = query_5(cursor,connection,'2003-2-5','2003-2-18',3)
    print_query(results,cursor)
    print("**number 1 genre is romance probably because it is around valantines day**")
    results = query_5(cursor,connection,'2001-9-11','2001-11-11',3)
    print_query(results,cursor)
    print("** Tried to find connection to world event but doesn't seem so**")
    results = query_5(cursor,connection,'2011-5-11','2011-11-11',3)
    print_query(results,cursor)

    return


def main():
    connection = mysql.connector.connect(
    host="localhost",
    port=3305,
    user="talweiss2",
    password="talweiss30336",
    database="talweiss2",
    )
    cursor = connection.cursor()

    #examples for the quries:

    query_1_example(cursor,connection)
    query_2_example(cursor,connection)
    query_3_example(cursor,connection)
    query_4_example(cursor,connection)
    query_5_example(cursor,connection)

if __name__ == '__main__':
    main()
