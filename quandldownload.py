"""
This script will import quandl fx rates into our local database

"""
__author__ = 'Adam McDonald <amcdonald@v2.ventures>'
__version__ = '1.0.0'

import datetime
import quandl
import records

#API key to connect to Quandl, need to insert your key
quandl.ApiConfig.api_key = "xxx"

#Connects to  ODBC connection, need to insert your data source name
dsn_conn = 'DSN=xxx;Trusted_Connection=yes'
db = records.Database('mssql+pyodbc:///?odbc_connect=%s' % dsn_conn)

#Current date and time for the UploadDate
uploaddate = datetime.datetime.now()

#date to pull the fx rates
yesterday = datetime.date.today() - datetime.timedelta(1)
yesterday = yesterday.strftime('%Y-%m-%d')

#currencies to pull
currencies = ['EUR','GBP','NZD','BRL','CAD','CNY']

#delete rates from sql table for the selected day if they exist
db.query("delete from fx_rates where date = '" + yesterday + "'")

#loop through currencies, downloads into numpy and loads into sql table
for cur in currencies:
	data = quandl.get('CUR/'+cur,start_date=yesterday,end_date=yesterday,returns="numpy")
	Date = data[0][0].strftime('%Y-%m-%d')
	UploadDate = uploaddate
	Rate = round(1 / data[0][1],4)
	Currency = cur
	db.query('INSERT INTO fx_rates (Date,UploadDate,Rate,Currency) VALUES(:Date,:UploadDate,:Rate,:Currency)'
        ,Date=Date,UploadDate=UploadDate,Rate=Rate,Currency=Currency)

db.Close()
