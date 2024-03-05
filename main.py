# Librerías
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import pandas as pd

# Instancia de la aplicación

app = FastAPI()

# Lista de datasets necesarios para ejecutar la aplicación

dfDeveloper = pd.read_parquet("datafunc/developer.parquet")
dfUserData = pd.read_parquet("datafunc/userdata.parquet")
dfUxG = pd.read_parquet("datafunc/userbygenre.parquet")
dfBestDevs = pd.read_parquet("datafunc/bestDevs.parquet")
dfRevFeels = pd.read_parquet("datafunc/devRevAna.parquet")

# CONFIGURACIÓN API

@app.get("/", response_class=HTMLResponse)
async def home():
    home = """
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Inicio | API STEAM</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 0;
                background-color: #f4f4f4;
                color: #333;
            }
            header {
                background-color: #007bff;
                color: #fff;
                padding: 20px;
                text-align: center;
            }
            h1 {
                margin: 0;
                font-size: 36px;
            }
            .container {
                max-width: 800px;
                margin: 20px auto;
                padding: 0 20px;
            }
            .intro {
                margin-top: 20px;
                padding: 20px;
                background-color: #fff;
                border: 1px solid #ddd;
                border-radius: 5px;
            }
        </style>
    </head>
    <body>
        <header>
            <h1>API de Ejemplo</h1>
        </header>
        <div class="container">
            <div class="intro">
                <h2>Bienvenido a la API de stea;</h2>
                <p>Esta API proporciona un sistema de consulta de aplicaciones (especialmente videojuegos)
                que se encuentran en la plataforma STEAM.</p>
                <p>Para comenzar a usar la API dirigete a la barra de nagación y escribe /docs o por el contrario
                dirigete al siguiente enlace <a href="https://mlopssteam.up.railway.app/docs">Enlace a la API</a> </p>
            </div>
        </div>
    </body>
    </html>
    """
    return home


# Función developer
@app.get("/developer/{dev}")
def developer(dev:str):
    # Formateamos el string de entrada para un correcto filtrado
    dev = dev.upper()
    
    #Manejamos posibles errores:
    if dev not in dfDeveloper["Developer"].unique():
        return {"Error": "El developer no se encuentra en la base de datos"}
    
    #Filtramos el dataframe y solo nos quedamos con el developer que queremos.
    dfDev = dfDeveloper[dfDeveloper["Developer"] == dev]
    
    #Creamos la agrupación por año de lanzamiento y calculamos la cantidad de aplicaciones de dicho año
    countPerYear = dfDev.groupby("ReleaseYear").size().reset_index(name="Cantidad de Apps")
    
    #Creamos un dataframe con los años de lanzamiento que tienen aplicaciones Free == 0
    priceFreePerYear = dfDev[dfDev["Price"] == 0]
    priceFreePerYear = priceFreePerYear.groupby("ReleaseYear").size().reset_index(name="PriceFreeCount")
    
    #Creamos un inner join que une countperYear con PriceFreeYear en la columna release year
    countPerYear = pd.merge(countPerYear, priceFreePerYear, on="ReleaseYear", how="left")
    
    # Creamos la tabla contenido free que es la cantidad de apps con precio gratis sobre el total de aplicacciones.
    countPerYear["Contenido Free"] = round((countPerYear["PriceFreeCount"] / countPerYear["Cantidad de Apps"]) * 100, 2)
    
    # Llenamos los valores NAN resultado del inner join y los ponemos en 0.
    countPerYear = countPerYear.fillna(0)
    
    dfDev = countPerYear[["ReleaseYear", "Cantidad de Apps", "Contenido Free"]].rename(columns={"ReleaseYear":"Año"})
    
    # Convertir el DataFrame a una lista de diccionarios
    result_dict = dfDev.to_dict(orient='records')
    
    return result_dict

# Función 2
@app.get("/userdata/{idUser}")
def userdata(idUser:str):
    # Formateamos el string de entrada a número.
    idUser = int(idUser)
    
    #Manejamos posibles errores:
    if idUser not in dfUserData["IdUserSteam"].unique():
        return {"Error": "El id de usuario no se encuentra en la base de datos"}
        
    #Filtramos el dataframe y solo nos quedamos con el idusuario que queremos.
    dfUs = dfUserData[dfUserData["IdUserSteam"] == idUser]
    
    #Agrupamos por el usuario indicado.
    groupUs = dfUs.groupby("IdUserSteam")
    
    #Calculamos la suma de los precios de los items que adquirió el usuario.
    totPrice = groupUs["Price"].sum().reset_index(name="Dinero Gastado")
    
    #Calculamos el promedio de recomendación de las apps que el usuario compró.
    avgRecom = round(groupUs["RecommendsPercen"].mean().reset_index(name="Porcentaje de Recomendación"),2)
    
    # Contamos la cantidad de items que compró el usuario.
    purchasedItems = groupUs.size().reset_index(name='Cantidad de items')

    # Unimos los resultados de las consultas.
    dfUs = pd.merge(totPrice, avgRecom, on='IdUserSteam')
    dfUs = pd.merge(dfUs, purchasedItems, on='IdUserSteam')
    
    result_dict = dfUs.to_dict(orient='records')
    
    return result_dict

# Función 3
@app.get("/userforgenre/{genre}")
def userForGenre(genre:str):
    # Formateamos el string de entrada y lo capitalizamos.
    genre = genre.upper()
    
    #Manejamos posibles errores:
    if genre not in dfUxG.columns:
        return {"Error": "El genero ingresado no se encuentra en la base de datos"}

    #Filtramos el dataframe y solo nos quedamos con el genero que queremos y las filas que solo correspondan a ese genero.
    data = dfUxG[["IdUserSteam","PlayTime","ReleaseYear", genre]].copy()
    data = data[data[genre]==1]
    data.drop(columns=genre, inplace=True)
    
    #Agrupamos por usuario y calculamos la suma de horas jugadas por cada usuario.
    groupByUs = dfUxG.groupby('IdUserSteam')['PlayTime'].sum().reset_index()
    
    #Encontramos el usuario con mayor número de horas de playtime usando la funcion idxmax() que permite encontrar el valor max.
    userMax = groupByUs.loc[groupByUs['PlayTime'].idxmax()]
    
    #Creamos las columnas por año y calculamos las sumas por año de ese usuario. Usamos pivot para convertir los valores en columnas
    dfMax = data[data['IdUserSteam'] == userMax['IdUserSteam']]
    dfPivoteado = dfMax.pivot_table(index='IdUserSteam', columns='ReleaseYear', values='PlayTime', aggfunc='sum').fillna(0)
    
    result_dict = dfPivoteado.to_dict(orient='records')
    
    # Mostrar el resultado
    print({f"Usuario con más horas jugadas para genero {genre}:" : userMax["IdUserSteam"]})
    print({"Total de horas jugadas del usuario: ": userMax['PlayTime']})
    return result_dict

# Función 4
@app.get("/bestdeveloper/{year}")
def bestDeveloper(year:str):
    
    if year not in dfBestDevs["ReleaseYear"].unique():
        return {"Error": "El año ingresado no está presente en la base de datos"}
    
    #Filtramos el dataframe y solo nos quedamos con el año que queremos.
    data = dfBestDevs[dfBestDevs["ReleaseYear"]==year]
    data = data.drop(columns="IdApp")
    
    #Agrupamos por usuario y calculamos la suma de recomendaciones por developer.
    groupByDev = data.groupby("Developer")['TrueCount'].sum().reset_index()
    
    #Ordenamos los resultados de mayor a menor y filtramos solo los 3 primeros
    bestdev = groupByDev.sort_values(by="TrueCount",ascending=False).head(3)
    
    #Reseatamos los indices y los convertimos en una columna de Ranking
    bestdev.reset_index(drop=True, inplace=True)
    bestdev.index += 1
    bestdev.reset_index(inplace=True)
    bestdev.rename(columns={'index': 'Puesto'}, inplace=True)
    
    #Eliminamos columna TrueCount
    bestdev.drop(columns="TrueCount", inplace=True)
    
    result_dict = bestdev.to_dict(orient='records')
    
    return result_dict


# Función 5
@app.get("/developerReviewAnalysis/{developer}")
def developerReviewsAnalysis(developer:str):

    developer = developer.upper()
    
    if developer not in dfRevFeels["Developer"].unique():
        return {"Error": "El desarrollador ingresado no está presente en la base de datos."}
    
    #Filtramos el dataframe y solo nos quedamos con el developer que queremos.
    data = dfRevFeels[dfRevFeels["Developer"]==developer]
    data = data.drop(columns=["IdApp", "Developer"])
    
    #Transformamos la columna SenAn1 en columnas dummies
    dummy = pd.get_dummies(data['SenAn1'])
    dummy.drop(columns=1, inplace=True)
    dummy.rename(columns={0 : "Negative", 2 : "Positive"}, inplace=True)
    
    result_dict = dummy.sum().to_dict()

    return result_dict