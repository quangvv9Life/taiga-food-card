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

sql4 = """

SELECT * FROM (SELECT DISTINCT LT5WI.id,LT5WI.name, ig.NameVIE,ig.NameENG,ig.FinalName,ig.OriginalWeight,ig.Calucateweight, ig.Calucateunit,ig.MaxCalucateWeight,ig.SubstituteIds,nu.unit,nu.isverified,

		UNNEST(ARRAY[
			'nu.calories','nu.sodium','nu.totalCarbs','nu.totalFat','nu.potassium','nu.saturated','nu.monounsaturated','nu.polyunsaturated','nu.dietaryFiber','nu.sugars','nu.trans','nu.protein','nu.cholesterol',
			'nu.vitaminA','nu.vitaminC','nu.calcium','nu.iron']
		) AS Nutritions,

		UNNEST(ARRAY[
			nu.calories,nu.sodium,nu.totalCarbs,nu.totalFat,nu.potassium,nu.saturated,nu.monounsaturated,nu.polyunsaturated,nu.dietaryFiber,nu.sugars,nu.trans,nu.protein,nu.cholesterol, nu.vitaminA,nu.vitaminC,nu.calcium,nu.iron]
		) AS Values,
		--ROW_NUMBER() OVER (ORDER BY LT5WI.id ASC) AS id_rank
		--ROW_NUMBER() OVER (PARTITION BY LT5WI.id ORDER BY LT5WI.id ASC) AS id_rank
		DENSE_RANK() OVER (ORDER BY LT5WI.id ASC) AS id_rank
	FROM LessThan5wIngre LT5WI
	JOIN IngredientRef re ON LT5WI.id = re.CookyFoodId
	JOIN IngredientTrans ig ON re.IngredientId = ig.Id
	JOIN IngredientNutrient nu ON ig.NameENG = nu.key
	ORDER BY id
) AS P
WHERE 1 = 1
AND Nutritions in ('nu.calories','nu.totalCarbs','nu.totalFat','nu.dietaryFiber','nu.sugars','nu.protein')
AND id_rank <= 10
AND id = %s
ORDER BY id
;

"""

# Select ID

sql5 = """

SELECT DISTINCT id FROM (SELECT DISTINCT LT5WI.id,LT5WI.name, ig.NameVIE,ig.NameENG,ig.FinalName,ig.OriginalWeight,ig.Calucateweight, ig.Calucateunit,ig.MaxCalucateWeight,ig.SubstituteIds,nu.unit,nu.isverified,

		UNNEST(ARRAY[
			'nu.calories','nu.sodium','nu.totalCarbs','nu.totalFat','nu.potassium','nu.saturated','nu.monounsaturated','nu.polyunsaturated','nu.dietaryFiber','nu.sugars','nu.trans','nu.protein','nu.cholesterol',
			'nu.vitaminA','nu.vitaminC','nu.calcium','nu.iron']
		) AS Nutritions,

		UNNEST(ARRAY[
			nu.calories,nu.sodium,nu.totalCarbs,nu.totalFat,nu.potassium,nu.saturated,nu.monounsaturated,nu.polyunsaturated,nu.dietaryFiber,nu.sugars,nu.trans,nu.protein,nu.cholesterol, nu.vitaminA,nu.vitaminC,nu.calcium,nu.iron]
		) AS Values,
		--ROW_NUMBER() OVER (ORDER BY LT5WI.id ASC) AS id_rank
		--ROW_NUMBER() OVER (PARTITION BY LT5WI.id ORDER BY LT5WI.id ASC) AS id_rank
		DENSE_RANK() OVER (ORDER BY LT5WI.id ASC) AS id_rank
	FROM LessThan5wIngre LT5WI
	JOIN IngredientRef re ON LT5WI.id = re.CookyFoodId
	JOIN IngredientTrans ig ON re.IngredientId = ig.Id
	JOIN IngredientNutrient nu ON ig.NameENG = nu.key
	ORDER BY id
) AS P
WHERE 1 = 1
AND Nutritions in ('nu.calories','nu.totalCarbs','nu.totalFat','nu.dietaryFiber','nu.sugars','nu.protein')
AND id_rank <= 10
ORDER BY id
;

"""

##################################################
# Food data structure
##################################################

food_template = {

    'food_id'      : 'food_id'              ,

    'food_name'    : 'food_template'        ,

    'total_ingredient' :  0                 ,

    'food_total_carbs'    :  'NULL'         ,
    'food_total_protein'  :  'NULL'         ,
    'food_total_fat'      :  'NULL'         ,

    'ingredient_1' : {
       'ingredient_name'   : 'ingredient_1' ,
       'nu.calories'       : 'NULL'         ,
       'nu.dietaryFiber'   : 'NULL'         ,
       'nu.protein'        : 'NULL'         ,
       'nu.sugars'         : 'NULL'         ,
       'nu.totalCarbs'     : 'NULL'         ,
       'nu.totalFat'       : 'NULL'         ,
    },


    'ingredient_2' : {
       'ingredient_name'   : 'ingredient_2' ,
       'nu.calories'       : 'NULL'         ,
       'nu.dietaryFiber'   : 'NULL'         ,
       'nu.protein'        : 'NULL'         ,
       'nu.sugars'         : 'NULL'         ,
       'nu.totalCarbs'     : 'NULL'         ,
       'nu.totalFat'       : 'NULL'         ,
    },

    'ingredient_3' : {
       'ingredient_name'   : 'ingredient_3' ,
       'nu.calories'       : 'NULL'         ,
       'nu.dietaryFiber'   : 'NULL'         ,
       'nu.protein'        : 'NULL'         ,
       'nu.sugars'         : 'NULL'         ,
       'nu.totalCarbs'     : 'NULL'         ,
       'nu.totalFat'       : 'NULL'         ,
    },

    'ingredient_4' : {
       'ingredient_name'   : 'ingredient_4' ,
       'nu.calories'       : 'NULL'         ,
       'nu.dietaryFiber'   : 'NULL'         ,
       'nu.protein'        : 'NULL'         ,
       'nu.sugars'         : 'NULL'         ,
       'nu.totalCarbs'     : 'NULL'         ,
       'nu.totalFat'       : 'NULL'         ,
    },

    'ingredient_5' : {
       'ingredient_name'   : 'ingredient_5' ,
       'nu.calories'       : 'NULL'         ,
       'nu.dietaryFiber'   : 'NULL'         ,
       'nu.protein'        : 'NULL'         ,
       'nu.sugars'         : 'NULL'         ,
       'nu.totalCarbs'     : 'NULL'         ,
       'nu.totalFat'       : 'NULL'         ,
    },

}

##################################################
# Functions
##################################################

def print_sql_out(i_rows):
    for row in i_rows:
        print (row)

def parse_data_one(i_row):
    out = {}
    out['food_id']    = i_row[0]
    out['food_name']  = i_row[1]
    out['ingredient'] = i_row[2]
    out['nutrition']  = i_row[12]
    out['nutrition_amount']  = i_row[13]

    return out

def parse_data(i_row, i_nth):
    out = {}
    out['food_id']    = i_row[i_nth][0]
    out['food_name']  = i_row[i_nth][1]
    out['ingredient'] = i_row[i_nth][2]
    out['nutrition']  = i_row[i_nth][12]
    out['nutrition_amount']  = i_row[i_nth][13]

    return out

def parse_food(i_rows):
    i = 0

    o_food_card = dict(food_template)

    for row in i_rows:
        out_data   = parse_data_one(row)

        print (out_data)

        ingredient_num = i // 6 + 1

        str_ingredient_num = 'ingredient' + '_' + str(ingredient_num)

        o_food_card['food_id']   = out_data['food_id']
        o_food_card['food_name'] = out_data['food_name']

        o_food_card[str_ingredient_num]['ingredient_name']       = out_data['ingredient']

        o_food_card[str_ingredient_num][out_data['nutrition']]   = str(out_data['nutrition_amount'])

        o_food_card['total_ingredient']                          = ingredient_num

        i = i + 1

    print(o_food_card)

    return o_food_card

def prepare_card(i_project_id, i_data):

    o_payload = json.dumps({
        "project" : i_project_id      ,
        "subject" : i_data['subject'] + '-' + i_data['nutrition']
    })

    print (o_payload)

    return o_payload

def send_card(i_url, i_payload):
    o_response = requests.request("POST", i_url, headers=headers, data=i_payload);

    print (headers)
    print (o_response)

    return o_response

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
            curs.execute(sql4, (34,))
            # print(curs.fetchall())
            # for row in curs.fetchmany(20):
            #     print (row[1])
            # print (curs.fetchmany(20))
            # print (type(curs.fetchmany(20)))

            # row_fetch = 40
            # row = curs.fetchmany(row_fetch)

            rows = curs.fetchall()

            print_sql_out(rows)

          # for i in range(row_fetch):
            food_card = parse_food(rows)

            project_id = 3
          # payload = prepare_card(project_id, out_data)

          # send_card(url_user_stories, payload)

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

