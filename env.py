env = 'prod'
#homolog
#prod
#teste commit
#Database credentials
DB_ADDRESS_ETL = '...'
DB_DATABASE_ETL = '...'
DB_USERNAME_ETL = '...'
DB_PASSWORD_ETL = '...'

#Conx string create based on given info
CONX_STRING_ETL = ('Driver={SQL Server};'
        'Server=' + DB_ADDRESS_ETL +
        ';Database=' + DB_DATABASE_ETL +
        ';UID=' + DB_USERNAME_ETL +
        ';PWD=' + DB_PASSWORD_ETL + ';')

if env == 'prod':
    ##############  PROD
    baseUrl = 'apiprod'

    #Database credentials
    DB_ADDRESS = '...'
    DB_DATABASE = '...'
    DB_USERNAME = '...'
    DB_PASSWORD = '...'



    #Conx string create based on given info
    CONX_STRING = ('Driver={SQL Server};'
            'Server=' + DB_ADDRESS +
            ';Database=' + DB_DATABASE +
            ';UID=' + DB_USERNAME +
            ';PWD=' + DB_PASSWORD + ';')  

elif env == 'homolog':
    ################    	HOMOLOG
    baseUrl = 'apihomolog'   

    #Database credentials
    DB_ADDRESS = '...'
    DB_DATABASE = '...'
    DB_USERNAME = '...'
    DB_PASSWORD = '...'

    #Conx string create based on given info
    CONX_STRING = ('Driver={SQL Server};'
            'Server=' + DB_ADDRESS +
            ';Database=' + DB_DATABASE +
            ';UID=' + DB_USERNAME +
            ';PWD=' + DB_PASSWORD + ';')
   