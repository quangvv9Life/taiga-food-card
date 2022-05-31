#!/usr/bin/python3

# References:
#  https://www.postgresqltutorial.com/postgresql-python/

import psycopg2

def select():

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
            print(curs.fetchall())

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()

    return updated_rows

if __name__ == '__main__':
    # Update vendor id 1
    select()
