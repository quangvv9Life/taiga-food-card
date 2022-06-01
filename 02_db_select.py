#!/usr/bin/python3

# References:
#  https://www.postgresqltutorial.com/postgresql-python/

import psycopg2
import json
import requests

##################################################
# Global for requests
##################################################

AUTH_TOKEN = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNjU0MDc5NDA0LCJqdGkiOiI4YzdlY2ZmOTA0YTI0MmJhYmMzNzA2MGEzMzkyYmIxMSIsInVzZXJfaWQiOjV9.hW-PSDKWGxn0qxfdpo0WVLlg8-_CDuOZPrJgqkywARs'

headers = {
  "Content-Type": "application/json",
  "Authorization": AUTH_TOKEN
}


url_user_stories = "http://9net.ddns.net:9000/api/v1/userstories"

##################################################
# Global for database
##################################################

""" select food """

sql1 = """
    CREATE VIEW LessThan5wIngre AS (
    SELECT a.id,a.Name,a.TotalTime,b.Name AS IngreName,b.Id AS IngreID,a.TotalLiked,a.AvgRating,a.DetailUrl
    FROM ingredientref r
    JOIN LessThanFive a  ON a.id= r.cookyfoodid
    JOIN ingredient b ON r.ingredientid = b.id
    GROUP BY a.name,a.TotalTime,a.Id,b.Name ,b.Id,a.TotalLiked,a.AvgRating,a.DetailUrl
    ORDER BY id
    );
"""

sql2 = """

   SELECT DISTINCT LT5WI.id,LT5WI.name, ig.NameVIE,ig.NameENG,ig.FinalName,ig.OriginalWeight,ig.Calucateweight, ig.Calucateunit,ig.MaxCalucateWeight,ig.SubstituteIds,nu.unit,nu.isverified,

          UNNEST(ARRAY[
                 'nu.calories','nu.sodium','nu.totalCarbs','nu.totalFat','nu.potassium','nu.saturated','nu.monounsaturated','nu.polyunsaturated','nu.dietaryFiber','nu.sugars','nu.trans','nu.protein','nu.cholesterol',
                 'nu.vitaminA','nu.vitaminC','nu.calcium','nu.iron']
          ) AS Nutritions,

          UNNEST(ARRAY[
                 nu.calories,nu.sodium,nu.totalCarbs,nu.totalFat,nu.potassium,nu.saturated,nu.monounsaturated,nu.polyunsaturated,nu.dietaryFiber,nu.sugars,nu.trans,nu.protein,nu.cholesterol,
                 nu.vitaminA,nu.vitaminC,nu.calcium,nu.iron]
          ) AS Values

   FROM LessThan5wIngre LT5WI
    JOIN IngredientRef re ON LT5WI.id = re.CookyFoodId
    JOIN IngredientTrans ig ON re.IngredientId = ig.Id
    JOIN IngredientNutrient nu ON ig.NameENG = nu.key
    WHERE 1=1
   ORDER BY id;

"""

sql3 = """
     SELECT * FROM LessThan5wIngre ;
"""

##################################################
# Functions
##################################################

def send_card(i_url, i_payload):
    o_response = requests.request("POST", i_url, headers=headers, data=i_payload);

    print (headers)
    print (o_response)

    return o_response

def parse_data(i_row):
    out = {}
    out['subject']   = i_row[0][1]
    out['nutrition'] = i_row[0][12]

    return out

def prepare_card(i_project_id, i_subject):

    o_payload = json.dumps({
        "project" : i_project_id      ,
        "subject" : i_subject 
    })

    print (o_payload)

    return o_payload

def select():
    conn = None
    updated_rows = 0

    try:
        # connect to the PostgreSQL database

        conn = psycopg2.connect(
            host="localhost",
            database="taiga",
            port="9432",
            user="taiga",
            password="taiga")

#       print ( "*** Execute create view ***" )

#       with conn.cursor() as curs:
#           curs.execute(sql1)

        print ( "*** Execute select***" )

        with conn.cursor() as curs:
            curs.execute(sql2)
            # print(curs.fetchall())
            # for row in curs.fetchmany(20):
            #     print (row[1])
            # print (curs.fetchmany(20))
            # print (type(curs.fetchmany(20)))

            row_fetch = 4
            row = curs.fetchmany(row_fetch)

            print (row)

            for i in range(row_fetch):
                out_data   = parse_data(row)

                project_id = 3
                payload = prepare_card(project_id, out_data['subject'])

               #send_card(url_user_stories, payload)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()

    return updated_rows


##################################################
# Main
##################################################

if __name__ == '__main__':
    select()

